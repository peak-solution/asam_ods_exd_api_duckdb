"""Custom sidecar gRPC service for append operations."""

from __future__ import annotations

import logging
import os
import tempfile
import time
from datetime import timezone
from typing import Any
from urllib.parse import unquote, urlparse
from urllib.request import url2pathname

import duckdb
import grpc
from ods_exd_api_box import ExternalDataReader, exd_api

from protobuf_generated import values_append_pb2 as va
from protobuf_generated import values_append_pb2_grpc


class ValuesAppenderService(values_append_pb2_grpc.ValuesAppenderServicer):
    """Implements the ValuesAppender sidecar API.

    This initial implementation validates the request contract and acknowledges
    accepted write requests. DuckDB write mapping is intentionally staged as a
    follow-up step.
    """

    _SCHEMA = (
        'channel_id BIGINT, "index" DOUBLE, '
        "value UNION("
        "DT_DOUBLE DOUBLE, "
        "DT_FLOAT FLOAT, "
        "DT_SHORT SHORT, "
        "DT_LONG INT, "
        "DT_LONGLONG LONG, "
        "DT_STRING VARCHAR, "
        "DT_BOOLEAN BOOLEAN, "
        "DT_BYTE UTINYINT, "
        "DT_COMPLEX STRUCT(re FLOAT, im FLOAT), "
        "DT_DCOMPLEX STRUCT(re DOUBLE, im DOUBLE), "
        "DT_BYTESTR BLOB, "
        "DT_DATE VARCHAR"
        "), "
        "flag SHORT"
    )

    def __init__(self, reader: ExternalDataReader | None = None) -> None:
        self._log = logging.getLogger(__name__)
        self._reader = reader

    def Init(
        self,
        request: va.InitRequest,
        context: grpc.ServicerContext,
    ) -> va.InitResult:
        for idx, group_id in enumerate(request.group_ids):
            if group_id < 0:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"group_ids[{idx}] must be >= 0")

        unique_group_ids = sorted(set(request.group_ids))
        with tempfile.TemporaryDirectory() as tmp_dir:
            duckdb_path = os.path.join(tmp_dir, "initialized.duckdb")
            with duckdb.connect(duckdb_path, read_only=False) as con:
                for group_id in unique_group_ids:
                    self._ensure_table_exists(con, self._resolve_table_name(str(group_id)))

            with open(duckdb_path, "rb") as db_file:
                binary_content = db_file.read()

        self._log.info(
            "Initialized DuckDB file for %d group(s): %s",
            len(unique_group_ids),
            unique_group_ids,
        )
        return va.InitResult(duckdb_file_content=binary_content)

    def Append(
        self,
        request: va.AppendRequest,
        context: grpc.ServicerContext,
    ) -> va.AppendResult:
        if not request.handle.uuid:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "AppendRequest.handle.uuid must not be empty")

        if len(request.values.values) == 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "WriteRequestList.values must not be empty")

        duckdb_path = self._resolve_duckdb_path_from_handle(request.handle, context)
        if not duckdb_path:
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                "No open DuckDB file found for handle. Call ExternalDataReader.Open first.",
            )

        profile_enabled = os.getenv("APPEND_PROFILE") == "1"
        profile_totals: dict[str, float] = {
            "extract_payload": 0.0,
            "extract_flags": 0.0,
            "next_index_query": 0.0,
            "insert_payload": 0.0,
        }

        appended_rows = 0
        touched_groups_to_tables: dict[str, str] = {}
        next_index_by_group: dict[str, int] = {}
        with duckdb.connect(duckdb_path, read_only=False) as con:
            batched_rows: dict[tuple[str, str], list[tuple[int, float, Any, int]]] = {}
            for idx, write_request in enumerate(request.values.values):
                group_id = str(write_request.channel_group_id)
                table_name = self._resolve_table_name(group_id)
                touched_groups_to_tables[group_id] = table_name

                if group_id not in next_index_by_group:
                    started_at = time.perf_counter()
                    next_index_by_group[group_id] = self._next_index_for_channel(con, table_name)
                    if profile_enabled:
                        profile_totals["next_index_query"] += time.perf_counter() - started_at

                start_index = next_index_by_group[group_id]
                member, rows = self._append_one_request(
                    con,
                    idx,
                    write_request,
                    context,
                    start_index,
                    profile_totals if profile_enabled else None,
                )
                appended_count = len(rows)
                batch_key = (table_name, member)
                batched_rows.setdefault(batch_key, []).extend(rows)
                appended_rows += appended_count
                next_index_by_group[group_id] += appended_count

            for (table_name, member), rows in batched_rows.items():
                started_at = time.perf_counter()
                self._insert_payload_rows(con, table_name, member, rows)
                if profile_enabled:
                    profile_totals["insert_payload"] += time.perf_counter() - started_at

            row_count_by_group = {
                group_id: self._number_of_rows_for_table(con, table_name)
                for group_id, table_name in sorted(touched_groups_to_tables.items())
            }

        self._log.info(
            (
                "Appended %d write request(s) / %d row(s); returning number_of_rows per "
                "table=%s in DuckDB file %s and handle %s"
            ),
            len(request.values.values),
            appended_rows,
            row_count_by_group,
            duckdb_path,
            request.handle.uuid,
        )
        result = va.AppendResult()
        result.row_count_by_group.update(row_count_by_group)

        if profile_enabled:
            total_profiled = sum(profile_totals.values())
            if total_profiled > 0:
                profile_line = (
                    "Append profile: "
                    f"extract_payload={profile_totals['extract_payload']:.3f}s "
                    f"({100.0 * profile_totals['extract_payload'] / total_profiled:.1f}%), "
                    f"extract_flags={profile_totals['extract_flags']:.3f}s "
                    f"({100.0 * profile_totals['extract_flags'] / total_profiled:.1f}%), "
                    f"next_index_query={profile_totals['next_index_query']:.3f}s "
                    f"({100.0 * profile_totals['next_index_query'] / total_profiled:.1f}%), "
                    f"insert_payload={profile_totals['insert_payload']:.3f}s "
                    f"({100.0 * profile_totals['insert_payload'] / total_profiled:.1f}%)"
                )
                self._log.info(
                    (
                        "Append profile: extract_payload=%.3fs (%.1f%%), extract_flags=%.3fs (%.1f%%), "
                        "next_index_query=%.3fs (%.1f%%), insert_payload=%.3fs (%.1f%%)"
                    ),
                    profile_totals["extract_payload"],
                    100.0 * profile_totals["extract_payload"] / total_profiled,
                    profile_totals["extract_flags"],
                    100.0 * profile_totals["extract_flags"] / total_profiled,
                    profile_totals["next_index_query"],
                    100.0 * profile_totals["next_index_query"] / total_profiled,
                    profile_totals["insert_payload"],
                    100.0 * profile_totals["insert_payload"] / total_profiled,
                )
                print(profile_line)

        return result

    def _resolve_duckdb_path_from_handle(
        self,
        handle: Any,
        context: grpc.ServicerContext,
    ) -> str | None:
        if self._reader is None:
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                "ValuesAppenderService requires a shared ExternalDataReader instance.",
            )

        identifier = self._reader.connection_map.get(handle.uuid)
        if identifier is None:
            return None

        return self._identifier_to_path(identifier)

    @staticmethod
    def _identifier_to_path(identifier: exd_api.Identifier) -> str:
        parsed = urlparse(identifier.url)
        if parsed.scheme:
            host = f"{os.path.sep}{os.path.sep}{parsed.netloc}{os.path.sep}"
            return os.path.normpath(os.path.join(host, url2pathname(unquote(parsed.path))))
        return identifier.url

    def _append_one_request(
        self,
        con: duckdb.DuckDBPyConnection,
        idx: int,
        write_request: Any,
        context: grpc.ServicerContext,
        start_index: int,
        profile_totals: dict[str, float] | None = None,
    ) -> tuple[str, list[tuple[int, float, Any, int]]]:
        if not write_request.channel_group_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"values[{idx}].channel_group_id is required")
        if not write_request.channel_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"values[{idx}].channel_id is required")
        if write_request.WhichOneof("data") is None:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"values[{idx}] must set one data variant")

        table_name = self._resolve_table_name(write_request.channel_group_id)
        self._ensure_table_exists(con, table_name)

        try:
            channel_id = int(write_request.channel_id)
        except ValueError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"values[{idx}].channel_id must be an integer: {exc}")

        started_at = time.perf_counter()
        member, payload_values = self._extract_explicit_payload(write_request, idx, context)
        if profile_totals is not None:
            profile_totals["extract_payload"] += time.perf_counter() - started_at
        if not payload_values:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"values[{idx}] explicit payload must not be empty")

        started_at = time.perf_counter()
        flags = self._extract_flags(write_request, len(payload_values), idx, context)
        if profile_totals is not None:
            profile_totals["extract_flags"] += time.perf_counter() - started_at

        rows = [
            (channel_id, float(start_index + offset), payload_values[offset], int(flags[offset]))
            for offset in range(len(payload_values))
        ]
        return member, rows

    def _resolve_table_name(self, channel_group_id: str) -> str:
        if channel_group_id.isdigit():
            return f"sm{channel_group_id}"
        return channel_group_id

    def _ensure_table_exists(self, con: duckdb.DuckDBPyConnection, table_name: str) -> None:
        quoted_table = self._quote_ident(table_name)
        con.execute(f"CREATE TABLE IF NOT EXISTS {quoted_table} ({self._SCHEMA})")

    def _extract_explicit_payload(
        self,
        write_request: Any,
        idx: int,
        context: grpc.ServicerContext,
    ) -> tuple[str, list[Any]]:
        data_variant = write_request.WhichOneof("data")
        if data_variant != "explicit":
            context.abort(
                grpc.StatusCode.UNIMPLEMENTED,
                f"values[{idx}] currently supports only explicit data payloads",
            )

        explicit = write_request.explicit
        explicit_variant = explicit.WhichOneof("values")
        if explicit_variant is None:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"values[{idx}].explicit must set one value array")

        if explicit_variant == "double_array":
            return "DT_DOUBLE", list(explicit.double_array.values)
        if explicit_variant == "float_array":
            return "DT_FLOAT", list(explicit.float_array.values)
        if explicit_variant == "long_array":
            return "DT_LONGLONG", list(explicit.long_array.values)
        if explicit_variant == "integer_array":
            return "DT_LONG", list(explicit.integer_array.values)
        if explicit_variant == "short_array":
            return "DT_SHORT", list(explicit.short_array.values)
        if explicit_variant == "byte_array":
            return "DT_BYTE", list(explicit.byte_array.values)
        if explicit_variant == "boolean_array":
            return "DT_BOOLEAN", list(explicit.boolean_array.values)
        if explicit_variant == "string_array":
            return "DT_STRING", list(explicit.string_array.values)
        if explicit_variant == "byte_stream_array":
            return "DT_BYTESTR", list(explicit.byte_stream_array.values)
        if explicit_variant == "float_complex_array":
            return "DT_COMPLEX", [{"re": v.re, "im": v.im} for v in explicit.float_complex_array.values]
        if explicit_variant == "double_complex_array":
            return "DT_DCOMPLEX", [{"re": v.re, "im": v.im} for v in explicit.double_complex_array.values]
        if explicit_variant == "date_array":
            iso_values = [
                v.ToDatetime().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
                for v in explicit.date_array.values
            ]
            return "DT_DATE", iso_values

        context.abort(
            grpc.StatusCode.UNIMPLEMENTED,
            f"values[{idx}] explicit variant {explicit_variant} is not supported",
        )

    def _extract_flags(
        self,
        write_request: Any,
        payload_len: int,
        idx: int,
        context: grpc.ServicerContext,
    ) -> list[int]:
        explicit = write_request.explicit
        if explicit.flags_full:
            flags = list(explicit.flags_full)
        elif explicit.flags:
            flags = [1 if flag else 0 for flag in explicit.flags]
        else:
            flags = [0] * payload_len

        if len(flags) not in (0, payload_len):
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"values[{idx}] flags length {len(flags)} does not match payload length {payload_len}",
            )

        if len(flags) == 0:
            return [0] * payload_len
        return flags

    def _next_index_for_channel(self, con: duckdb.DuckDBPyConnection, table_name: str) -> int:
        quoted_table = self._quote_ident(table_name)
        row = con.execute(f'SELECT COALESCE(MAX("index"), -1) FROM {quoted_table}').fetchone()
        if row is None:
            return 0
        return int(float(row[0])) + 1

    def _number_of_rows_for_table(self, con: duckdb.DuckDBPyConnection, table_name: str) -> int:
        quoted_table = self._quote_ident(table_name)
        row = con.execute(f'SELECT COALESCE(MAX(CAST("index" AS BIGINT)), -1) FROM {quoted_table}').fetchone()
        if row is None:
            return 0
        return int(row[0]) + 1

    def _insert_payload_rows(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        member: str,
        rows: list[tuple[int, float, Any, int]],
    ) -> None:
        quoted_table = self._quote_ident(table_name)
        sql = f"INSERT INTO {quoted_table} VALUES (?, ?, union_value({member} := ?), ?)"
        con.executemany(sql, rows)

    @staticmethod
    def _quote_ident(identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'
