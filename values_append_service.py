"""Custom sidecar gRPC service for append operations."""

from __future__ import annotations

import logging
import os
import tempfile
from datetime import timezone
from typing import Any
from urllib.parse import unquote, urlparse
from urllib.request import url2pathname

import duckdb
import grpc
import pandas  # pyright: ignore[reportMissingTypeStubs]
from ods_exd_api_box import ExternalDataReader, exd_api

from protobuf_generated import values_append_pb2 as va
from protobuf_generated import values_append_pb2_grpc


class ValuesAppenderService(values_append_pb2_grpc.ValuesAppenderServicer):
    """Implements the ValuesAppender sidecar API.

    This initial implementation validates the request contract and acknowledges
    accepted write requests. DuckDB write mapping is intentionally staged as a
    follow-up step.
    """

    _SUPPORTED_MEMBERS = {
        "DT_DOUBLE",
        "DT_FLOAT",
        "DT_SHORT",
        "DT_LONG",
        "DT_LONGLONG",
        "DT_STRING",
        "DT_BOOLEAN",
        "DT_BYTE",
        "DT_BYTESTR",
        "DT_DATE",
        "DT_COMPLEX",
        "DT_DCOMPLEX",
    }

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

        grouped_write_requests: dict[str, list[Any]] = {}
        group_value_lengths: dict[str, int] = {}
        appended_rows = 0

        for idx, write_request in enumerate(request.values.values):
            if not write_request.channel_group_id:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"values[{idx}].channel_group_id is required")
            if write_request.WhichOneof("data") is None:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"values[{idx}] must set one data variant")

            _, payload_values = self._extract_explicit_payload(write_request, idx, context)
            if not payload_values:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"values[{idx}] explicit payload must not be empty")

            group_id = str(write_request.channel_group_id)
            payload_length = len(payload_values)
            expected_length = group_value_lengths.get(group_id)
            if expected_length is None:
                group_value_lengths[group_id] = payload_length
            elif expected_length != payload_length:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    (
                        f"channel_group_id {group_id} has mismatched payload lengths: "
                        f"expected {expected_length}, got {payload_length}"
                    ),
                )

            grouped_write_requests.setdefault(group_id, []).append(write_request)

        max_index_by_group: dict[str, int] = {}
        with duckdb.connect(duckdb_path, read_only=False) as con:
            for group_id, write_requests in grouped_write_requests.items():
                table_name = self._resolve_table_name(group_id)
                self._ensure_table_exists(con, table_name)

                start_index = self.next_index_for_table(con, table_name)
                group_dataframe = self._write_requests_to_dataframe(write_requests, context)

                group_dataframe["index"] = group_dataframe["index"] + float(start_index)
                self._insert_group_frame(con, table_name, group_dataframe)

                max_index_by_group[group_id] = self.next_index_for_table(con, table_name)
                appended_rows += len(group_dataframe)

        self._log.info(
            (
                "Appended %d write request(s) / %d row(s); returning max_index per "
                "table=%s in DuckDB file %s and handle %s"
            ),
            len(request.values.values),
            appended_rows,
            max_index_by_group,
            duckdb_path,
            request.handle.uuid,
        )
        result = va.AppendResult()
        result.row_count_by_group.update(max_index_by_group)

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

    def _resolve_table_name(self, channel_group_id: str) -> str:
        if channel_group_id.isdigit():
            return f"sm{channel_group_id}"
        return channel_group_id

    def _ensure_union_type_exists(self, con: duckdb.DuckDBPyConnection) -> None:
        con.execute("""CREATE TYPE IF NOT EXISTS exd_value_t AS UNION(
                DT_DOUBLE DOUBLE,
                DT_FLOAT FLOAT,
                DT_SHORT SHORT,
                DT_LONG INT,
                DT_LONGLONG LONG,
                DT_STRING VARCHAR,
                DT_BOOLEAN BOOLEAN,
                DT_BYTE UTINYINT,
                DT_COMPLEX STRUCT(re FLOAT, im FLOAT),
                DT_DCOMPLEX STRUCT(re DOUBLE, im DOUBLE),
                DT_BYTESTR BLOB,
                DT_DATE VARCHAR
            )""")

    def _ensure_table_exists(self, con: duckdb.DuckDBPyConnection, table_name: str) -> None:
        quoted_table = self._quote_ident(table_name)
        self._ensure_union_type_exists(con)
        con.execute(
            f"""CREATE TABLE IF NOT EXISTS {quoted_table}
                (channel_id BIGINT, "index" DOUBLE, value exd_value_t, flag SHORT)
            """
        )

    def _write_requests_to_dataframe(
        self,
        write_requests: list[Any],
        context: grpc.ServicerContext,
    ) -> pandas.DataFrame:
        """Flatten explicit write requests into a DataFrame for SQL inserts.

        The returned DataFrame always contains these columns:
        channel_id, index, dtype, value, flag.
        """

        rows: list[dict[str, Any]] = []

        for idx, write_request in enumerate(write_requests):
            if not write_request.channel_id:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"values[{idx}].channel_id is required")

            try:
                channel_id = int(write_request.channel_id)
            except ValueError as exc:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"values[{idx}].channel_id must be an integer: {exc}")

            dtype, payload_values = self._extract_explicit_payload(write_request, idx, context)
            if dtype not in self._SUPPORTED_MEMBERS:
                context.abort(grpc.StatusCode.UNIMPLEMENTED, f"values[{idx}] explicit dtype {dtype} is not supported")
            if not payload_values:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"values[{idx}] explicit payload must not be empty")

            flags = self._extract_flags(write_request, len(payload_values), idx, context)

            for offset, value in enumerate(payload_values):
                rows.append(
                    {
                        "channel_id": channel_id,
                        "index": float(offset),
                        "dtype": dtype,
                        "value": value,
                        "flag": int(flags[offset]),
                    }
                )

        return pandas.DataFrame(rows, columns=["channel_id", "index", "dtype", "value", "flag"])

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

    def next_index_for_table(self, con: duckdb.DuckDBPyConnection, table_name: str) -> int:
        quoted_table = self._quote_ident(table_name)
        row = con.execute(f'SELECT COALESCE(MAX(CAST("index" AS BIGINT)), -1) FROM {quoted_table}').fetchone()
        if row is None or row[0] is None:
            return -1
        return int(row[0]) + 1

    def _insert_group_frame(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        dataframe: pandas.DataFrame,
    ) -> None:
        if dataframe.empty:
            return

        # Split by dtype so each registered DataFrame has a homogeneous value
        # column.  DuckDB infers the column type from the first values it sees
        # in a registered pandas relation; a mixed-type (object) column would
        # cause DuckDB to pick a single concrete type and then fail when it
        # encounters values of a different Python type during materialisation.
        for dtype, sub_df in dataframe.groupby("dtype", sort=False):
            self._insert_dtype_frame(con, table_name, sub_df)

    def _insert_dtype_frame(
        self,
        con: duckdb.DuckDBPyConnection,
        table_name: str,
        dataframe: pandas.DataFrame,
    ) -> None:
        if dataframe.empty:
            return

        quoted_table = self._quote_ident(table_name)
        relation_name = "df"
        con.register(relation_name, dataframe)

        try:
            sql = f"""INSERT INTO {quoted_table} SELECT channel_id, "index", CASE
            WHEN dtype = 'DT_LONG' THEN CAST(union_value(DT_LONG := CAST(value AS INTEGER)) as exd_value_t)
            WHEN dtype = 'DT_DOUBLE' THEN CAST(union_value(DT_DOUBLE := CAST(value AS DOUBLE)) as exd_value_t)
            WHEN dtype = 'DT_FLOAT' THEN CAST(union_value(DT_FLOAT := CAST(value AS FLOAT)) as exd_value_t)
            WHEN dtype = 'DT_SHORT' THEN CAST(union_value(DT_SHORT := CAST(value AS SHORT)) as exd_value_t)
            WHEN dtype = 'DT_LONGLONG' THEN CAST(union_value(DT_LONGLONG := CAST(value AS LONG)) as exd_value_t)
            WHEN dtype = 'DT_STRING' THEN CAST(union_value(DT_STRING := CAST(value AS VARCHAR)) as exd_value_t)
            WHEN dtype = 'DT_BOOLEAN' THEN CAST(union_value(DT_BOOLEAN := CAST(value AS BOOLEAN)) as exd_value_t)
            WHEN dtype = 'DT_BYTE' THEN CAST(union_value(DT_BYTE := CAST(value AS UTINYINT)) as exd_value_t)
            WHEN dtype = 'DT_BYTESTR' THEN CAST(union_value(DT_BYTESTR := CAST(value AS BLOB)) as exd_value_t)
            WHEN dtype = 'DT_DATE' THEN CAST(union_value(DT_DATE := CAST(value AS VARCHAR)) as exd_value_t)
            WHEN dtype = 'DT_COMPLEX' THEN CAST(
                union_value(DT_COMPLEX := CAST(value AS STRUCT(re FLOAT, im FLOAT))) as exd_value_t
            )
            WHEN dtype = 'DT_DCOMPLEX' THEN CAST(
                union_value(DT_DCOMPLEX := CAST(value AS STRUCT(re DOUBLE, im DOUBLE))) as exd_value_t
            )
            ELSE CAST(union_value(DT_STRING := '') as exd_value_t)
            END, flag from {relation_name}"""
            con.execute(sql)
        finally:
            con.unregister(relation_name)

    @staticmethod
    def _quote_ident(identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'
