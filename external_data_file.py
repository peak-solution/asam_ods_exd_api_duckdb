"""ASAM ODS EXD API implementation for DuckDB files."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import Any, Mapping, cast, override

import duckdb
from ods_exd_api_box import ExdFileInterface, exd_api, ods


class ExternalDataFile(ExdFileInterface):
    """Class for handling DuckDB files."""

    _INDEPENDENT_COLUMN = "index"
    _GROUP_TABLE_PREFIX = "sm"
    _CHANNEL_COLUMN = "channel_id"
    _REQUIRED_COLUMNS = {_CHANNEL_COLUMN, "index", "value", "flag"}
    _MEMBER_TO_ODS = {
        "DT_DOUBLE": ods.DataTypeEnum.DT_DOUBLE,
        "DT_FLOAT": ods.DataTypeEnum.DT_FLOAT,
        "DT_SHORT": ods.DataTypeEnum.DT_SHORT,
        "DT_LONG": ods.DataTypeEnum.DT_LONG,
        "DT_LONGLONG": ods.DataTypeEnum.DT_LONGLONG,
        "DT_STRING": ods.DataTypeEnum.DT_STRING,
        "DT_BOOLEAN": ods.DataTypeEnum.DT_BOOLEAN,
        "DT_BYTE": ods.DataTypeEnum.DT_BYTE,
        "DT_COMPLEX": ods.DataTypeEnum.DT_COMPLEX,
        "DT_DCOMPLEX": ods.DataTypeEnum.DT_DCOMPLEX,
        "DT_BYTESTR": ods.DataTypeEnum.DT_BYTESTR,
        "DT_DATE": ods.DataTypeEnum.DT_DATE,
    }
    _MEMBER_ORDER = [
        "DT_DOUBLE",
        "DT_FLOAT",
        "DT_SHORT",
        "DT_LONG",
        "DT_LONGLONG",
        "DT_STRING",
        "DT_BOOLEAN",
        "DT_BYTE",
        "DT_COMPLEX",
        "DT_DCOMPLEX",
        "DT_BYTESTR",
        "DT_DATE",
    ]

    @dataclass(frozen=True)
    class _ChannelMeta:
        channel_id: int
        union_member: str
        ods_type: ods.DataTypeEnum

    @dataclass(frozen=True)
    class _TableMeta:
        index_type: ods.DataTypeEnum
        channels: list[ExternalDataFile._ChannelMeta]
        channels_by_id: dict[int, ExternalDataFile._ChannelMeta]

    @classmethod
    @override
    def create(cls, file_path: str, parameters: str) -> ExdFileInterface:
        """Factory method to create a file handler instance."""
        return cls(file_path, parameters)

    @override
    def __init__(self, file_path: str, parameters: str = ""):
        self._log = logging.getLogger(__name__)
        self._log.debug("Initializing ExternalDataFile with path: %s and parameters: %s", file_path, parameters)
        self.file_path = file_path
        self.parameters = parameters
        self._table_meta: dict[str, ExternalDataFile._TableMeta] = {}
        self._union_members: dict[str, dict[int, set[str]]] = {}
        # Must match appender connection mode so both services can access one file.
        self.con = duckdb.connect(file_path, read_only=False)
        self._log.debug("ExternalDataFile initialized")

    @override
    def close(self) -> None:
        """Close the external data file."""
        self.con.close()
        self._log.debug("ExternalDataFile closed")

    @override
    def fill_structure(self, structure: exd_api.StructureResult) -> None:
        """Fill structure for channel_id/index/value/flag row schema."""
        self._log.debug("Building structure for file: %s", self.file_path)

        start_time_ods = datetime.now().strftime("%Y%m%d%H%M%S%f")
        structure.attributes.variables["start_time"].string_array.values.append(start_time_ods)

        tables = self._list_tables()
        for group_index, table_name in enumerate(tables):
            table_meta = self._get_table_meta(table_name)
            row_count = self._get_max_index(table_name)

            new_group = exd_api.StructureResult.Group()
            new_group.name = table_name
            new_group.id = self._group_id_from_table_name(table_name, fallback=group_index)
            new_group.total_number_of_channels = len(table_meta.channels)
            new_group.number_of_rows = row_count

            for channel_index, channel_meta in enumerate(table_meta.channels):
                new_channel = exd_api.StructureResult.Channel()
                new_channel.name = str(channel_meta.channel_id)
                new_channel.id = channel_meta.channel_id
                new_channel.data_type = channel_meta.ods_type
                new_channel.unit_string = ""
                if channel_meta.channel_id == 0:
                    new_channel.attributes.variables["independent"].long_array.values.append(1)
                new_group.channels.append(new_channel)

            structure.groups.append(new_group)

    @override
    def get_values(self, request: exd_api.ValuesRequest) -> exd_api.ValuesResult:
        """Get values where samples are rows keyed by channel_id."""
        self._log.debug(
            "GetValues request group_id=%d start=%d limit=%d channel_count=%d",
            request.group_id,
            request.start,
            request.limit,
            len(request.channel_ids),
        )

        table_name = self._resolve_table_name_for_group_id(request.group_id)
        table_meta = self._get_table_meta(table_name)
        quoted_table = self.__quote_ident(table_name)
        quoted_index = self.__quote_ident(self._INDEPENDENT_COLUMN)

        nr_of_rows = self._get_max_index(table_name)
        if request.start > nr_of_rows:
            raise ValueError(f"Channel start index {request.start} out of range!")

        rv = exd_api.ValuesResult(id=request.group_id)

        requested_channel_ids = sorted(set(request.channel_ids))
        # Channels appended after this instance's cache was built are missed here;
        # refresh once so newly written channel ids become visible without a restart.
        if any(channel_id not in table_meta.channels_by_id for channel_id in requested_channel_ids):
            table_meta = self._get_table_meta(table_name, force_refresh=True)

        channel_meta_by_id: dict[int, ExternalDataFile._ChannelMeta] = {}
        for channel_id in requested_channel_ids:
            channel_meta = table_meta.channels_by_id.get(channel_id)
            if channel_meta is None:
                raise ValueError(f"Invalid channel id {channel_id}!")
            channel_meta_by_id[channel_id] = channel_meta

        stop = min(nr_of_rows, request.start + request.limit)
        values_by_channel: dict[int, list[object]] = {}
        flags_by_channel: dict[int, list[object]] = {}
        if stop > request.start and requested_channel_ids:
            quoted_value = self.__quote_ident("value")
            quoted_flag = self.__quote_ident("flag")
            query_rows = self.con.execute(
                f"WITH idx AS ("
                f"SELECT range AS sample_index FROM range(?, ?)"
                f"), channels AS ("
                f"SELECT unnest(?) AS channel_id"
                f") "
                f"SELECT "
                f"c.channel_id, "
                f"array_agg(t.{quoted_value} ORDER BY idx.sample_index) AS values, "
                f"array_agg(t.{quoted_flag} ORDER BY idx.sample_index) AS flags "
                f"FROM channels c "
                f"CROSS JOIN idx "
                f"LEFT JOIN {quoted_table} AS t "
                f"ON t.{self.__quote_ident(self._CHANNEL_COLUMN)} = c.channel_id "
                f"AND CAST(t.{quoted_index} AS BIGINT) = idx.sample_index "
                f"GROUP BY c.channel_id "
                f"ORDER BY c.channel_id",
                [request.start, stop, requested_channel_ids],
            ).fetchall()

            for channel_id_value, values_obj, flags_obj in query_rows:
                channel_id = int(channel_id_value)
                values_by_channel[channel_id] = list(values_obj) if values_obj is not None else []
                flags_by_channel[channel_id] = list(flags_obj) if flags_obj is not None else []

        for channel_id in requested_channel_ids:
            channel_meta = channel_meta_by_id[channel_id]
            ods_type = channel_meta.ods_type
            flags: list[int] = []
            section: list[object] = []

            channel_values = values_by_channel.get(channel_id, [])
            channel_flags = flags_by_channel.get(channel_id, [])
            for index, value_obj in enumerate(channel_values):
                flag_value = channel_flags[index] if index < len(channel_flags) else None
                if value_obj is None:
                    section.append(self._invalid_placeholder_for_type(ods_type))
                    flags.append(0)
                else:
                    section.append(self._extract_value(value_obj, channel_meta.union_member))
                    flags.append(int(flag_value) if flag_value is not None else 0)

            new_channel_values = exd_api.ValuesResult.ChannelValues()
            new_channel_values.id = channel_id
            new_channel_values.values.data_type = ods_type
            # Omit flags array when all values are 15 (ODS default semantics).
            if flags and not all(flag == 15 for flag in flags):
                new_channel_values.flags.values[:] = flags

            if ods_type == ods.DataTypeEnum.DT_BOOLEAN:
                new_channel_values.values.boolean_array.values.extend(section)
            elif ods_type == ods.DataTypeEnum.DT_BYTE:
                byte_values: list[int] = []
                for raw_value in section:
                    if not isinstance(raw_value, int):
                        raise ValueError(f"Expected integer byte value, got {raw_value!r}")
                    byte_values.append(raw_value)
                new_channel_values.values.byte_array.values = bytes(byte_values)
            elif ods_type in (ods.DataTypeEnum.DT_SHORT, ods.DataTypeEnum.DT_LONG):
                new_channel_values.values.long_array.values[:] = section
            elif ods_type == ods.DataTypeEnum.DT_LONGLONG:
                new_channel_values.values.longlong_array.values[:] = section
            elif ods_type == ods.DataTypeEnum.DT_FLOAT:
                new_channel_values.values.float_array.values[:] = section
            elif ods_type == ods.DataTypeEnum.DT_DOUBLE:
                new_channel_values.values.double_array.values[:] = section
            elif ods_type == ods.DataTypeEnum.DT_COMPLEX:
                interleaved: list[float] = []
                for v in section:
                    if not isinstance(v, Mapping):
                        raise ValueError(f"Expected complex struct mapping, got {v!r}")
                    struct_v = cast(Mapping[str, Any], v)
                    interleaved.append(float(struct_v["re"]))
                    interleaved.append(float(struct_v["im"]))
                new_channel_values.values.float_array.values[:] = interleaved
            elif ods_type == ods.DataTypeEnum.DT_DCOMPLEX:
                interleaved_d: list[float] = []
                for v in section:
                    if not isinstance(v, Mapping):
                        raise ValueError(f"Expected dcomplex struct mapping, got {v!r}")
                    struct_vd = cast(Mapping[str, Any], v)
                    interleaved_d.append(float(struct_vd["re"]))
                    interleaved_d.append(float(struct_vd["im"]))
                new_channel_values.values.double_array.values[:] = interleaved_d
            elif ods_type == ods.DataTypeEnum.DT_STRING:
                new_channel_values.values.string_array.values[:] = [str(v) for v in section]
            elif ods_type == ods.DataTypeEnum.DT_BYTESTR:
                for item in section:
                    if isinstance(item, bytes):
                        new_channel_values.values.bytestr_array.values.append(item)
                    elif isinstance(item, bytearray):
                        new_channel_values.values.bytestr_array.values.append(bytes(cast(bytearray, item)))
                    else:
                        raise ValueError(f"Expected bytes value, got {item!r}")
            elif ods_type == ods.DataTypeEnum.DT_DATE:
                new_channel_values.values.string_array.values[:] = [self._to_ods_date_string(v) for v in section]
            else:
                raise NotImplementedError(f"Unsupported ODS type {ods_type!r}")

            rv.channels.append(new_channel_values)

        return rv

    def _get_table_meta(self, table_name: str, force_refresh: bool = False) -> _TableMeta:
        """Build and cache channel mapping for one table."""
        if force_refresh:
            self._table_meta.pop(table_name, None)
            self._union_members.pop(table_name, None)
        elif table_name in self._table_meta:
            return self._table_meta[table_name]

        columns = self._list_columns(table_name)
        column_names = {name for name, _ in columns}
        if column_names != self._REQUIRED_COLUMNS:
            raise ValueError(
                f"Required columns are {sorted(self._REQUIRED_COLUMNS)!r}, "
                f"got {sorted(column_names)!r} for table {table_name!r}."
            )

        index_type_str = next(col_type for col_name, col_type in columns if col_name == self._INDEPENDENT_COLUMN)
        index_type = self.__duckdb_type_to_ods(index_type_str)

        quoted_table = self.__quote_ident(table_name)
        distinct_channel_ids = self.con.execute(
            f"SELECT DISTINCT {self.__quote_ident(self._CHANNEL_COLUMN)} FROM {quoted_table} ORDER BY 1"
        ).fetchall()

        channels: list[ExternalDataFile._ChannelMeta] = []
        for row in distinct_channel_ids:
            channel_id_value = row[0]
            if channel_id_value is None:
                continue
            channel_id = int(channel_id_value)
            union_member = self._detect_channel_union_member(table_name, channel_id)
            if union_member not in self._MEMBER_TO_ODS:
                raise ValueError(f"Unsupported union member {union_member!r} for channel id {channel_id!r}")

            channels.append(
                ExternalDataFile._ChannelMeta(
                    channel_id=channel_id,
                    union_member=union_member,
                    ods_type=self._MEMBER_TO_ODS[union_member],
                )
            )

        table_meta = ExternalDataFile._TableMeta(
            index_type=index_type,
            channels=channels,
            channels_by_id={channel.channel_id: channel for channel in channels},
        )
        self._table_meta[table_name] = table_meta
        return table_meta

    def _detect_channel_union_member(self, table_name: str, channel_id: int) -> str:
        """Resolve the active UNION member for one channel id."""
        quoted_table = self.__quote_ident(table_name)
        quoted_channel_id = self.__quote_ident(self._CHANNEL_COLUMN)
        quoted_value = self.__quote_ident("value")

        if table_name not in self._union_members:
            try:
                rows = self.con.execute(
                    f"SELECT {quoted_channel_id}, CAST(union_tag({quoted_value}) AS VARCHAR) "
                    f"FROM {quoted_table} GROUP BY {quoted_channel_id}, union_tag({quoted_value})"
                ).fetchall()
            except duckdb.Error:
                rows = []

            members_by_id: dict[int, set[str]] = {}
            for channel_id_value, member_value in rows:
                if channel_id_value is None or member_value is None:
                    continue
                channel_members = members_by_id.setdefault(int(channel_id_value), set())
                channel_members.add(str(member_value).upper())
            self._union_members[table_name] = members_by_id

        members = sorted(self._union_members.get(table_name, {}).get(channel_id, set()))
        if not members:
            sample = self.con.execute(
                f"SELECT {quoted_value} FROM {quoted_table} "
                f"WHERE {quoted_channel_id} = ? AND {quoted_value} IS NOT NULL LIMIT 1",
                [channel_id],
            ).fetchone()
            if sample is None:
                raise ValueError(f"Could not infer type for channel id {channel_id!r}")
            return self._detect_union_member(sample[0])
        if len(members) > 1:
            raise ValueError(f"Channel id {channel_id!r} uses mixed union members, which is not supported.")
        return members[0]

    def _detect_union_member(self, value: object) -> str:
        """Detect active member name in a DuckDB UNION value represented in Python."""
        if not isinstance(value, dict):
            if isinstance(value, str):
                return "DT_STRING"
            if isinstance(value, float):
                return "DT_DOUBLE"
            if isinstance(value, int):
                return "DT_LONG"
            raise ValueError(f"Could not infer union member from scalar value {value!r}")

        direct_members = [name for name in self._MEMBER_ORDER if name in value and value[name] is not None]
        if len(direct_members) == 1:
            return direct_members[0]

        tag_value = value.get("__tag")
        if isinstance(tag_value, str):
            return tag_value
        if isinstance(tag_value, int) and 0 <= tag_value < len(self._MEMBER_ORDER):
            return self._MEMBER_ORDER[tag_value]

        raise ValueError(f"Could not determine union member from value {value!r}")

    def _extract_value(self, value: object, member: str) -> object:
        """Extract active value payload from a DuckDB UNION dictionary."""
        if value is None:
            raise ValueError("NULL values are not supported")
        if not isinstance(value, dict):
            return value
        if member in ("DT_COMPLEX", "DT_DCOMPLEX") and "re" in value and "im" in value:
            return value
        if member not in value:
            raise ValueError(f"Union member {member!r} missing from value {value!r}")
        return value[member]

    def _get_max_index(self, table_name: str) -> int:
        """Return exclusive upper bound for index-based reads.

        This returns ``max(index) + 1`` (or ``0`` for empty tables), which
        matches the slicing logic used in ``get_values``.
        """
        quoted_table = self.__quote_ident(table_name)
        quoted_index = self.__quote_ident(self._INDEPENDENT_COLUMN)
        row = self.con.execute(
            f"SELECT COALESCE(MAX(CAST({quoted_index} AS BIGINT)), -1) FROM {quoted_table}"
        ).fetchone()
        if row is None:
            raise ValueError(f"Could not read max index for table {table_name!r}")
        max_index = cast(int, row[0])
        return int(max_index) + 1

    @staticmethod
    def _invalid_placeholder_for_type(ods_type: ods.DataTypeEnum) -> object:
        """Return placeholder value for missing samples in index-aligned retrieval."""
        if ods_type == ods.DataTypeEnum.DT_BOOLEAN:
            return False
        if ods_type in (ods.DataTypeEnum.DT_BYTE, ods.DataTypeEnum.DT_SHORT, ods.DataTypeEnum.DT_LONG):
            return 0
        if ods_type == ods.DataTypeEnum.DT_LONGLONG:
            return 0
        if ods_type in (ods.DataTypeEnum.DT_FLOAT, ods.DataTypeEnum.DT_DOUBLE):
            return 0.0
        if ods_type in (ods.DataTypeEnum.DT_COMPLEX, ods.DataTypeEnum.DT_DCOMPLEX):
            return {"re": 0.0, "im": 0.0}
        if ods_type in (ods.DataTypeEnum.DT_STRING, ods.DataTypeEnum.DT_DATE):
            return ""
        if ods_type == ods.DataTypeEnum.DT_BYTESTR:
            return b""
        return 0

    @staticmethod
    def _to_ods_date_string(value: object) -> str:
        """Convert DATE/TIME/TIMESTAMP-like values to ODS timestamp format."""
        if isinstance(value, datetime):
            return ExternalDataFile._datetime_to_ods_string(value)

        if isinstance(value, date):
            return ExternalDataFile._datetime_to_ods_string(datetime.combine(value, time.min))

        if isinstance(value, str):
            stripped = value.strip()
            if len(stripped) == 23 and stripped.isdigit():
                return stripped

            parsed = ExternalDataFile._parse_datetime_string(stripped)
            if parsed is not None:
                return ExternalDataFile._datetime_to_ods_string(parsed)
            return stripped

        return str(value)

    @staticmethod
    def _parse_datetime_string(value: str) -> datetime | None:
        """Parse common ISO-like datetime strings to a datetime object."""
        normalized = value
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"

        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None

    @staticmethod
    def _datetime_to_ods_string(value: datetime) -> str:
        """Format datetime to ODS layout: YYYYMMDDHHmmSSNNNNNNNNN."""
        dt = value
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)

        nanoseconds = dt.microsecond * 1000
        return dt.strftime("%Y%m%d%H%M%S") + f"{nanoseconds:09d}"

    def _list_tables(self) -> list[str]:
        """Return table names sorted alphabetically."""
        rows = self.con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_type = 'BASE TABLE' "
            "ORDER BY table_name"
        ).fetchall()
        return [row[0] for row in rows]

    def _list_columns(self, table_name: str) -> list[tuple[str, str]]:
        """Return (column_name, data_type) pairs for a table, in ordinal order."""
        rows = self.con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'main' AND table_name = ? "
            "ORDER BY ordinal_position",
            [table_name],
        ).fetchall()
        return [(row[0], row[1]) for row in rows]

    def _resolve_table_name_for_group_id(self, group_id: int) -> str:
        """Resolve group_id to table name, preferring `sm<group_id>` naming convention."""
        tables = self._list_tables()
        if group_id < 0:
            raise ValueError(f"Invalid group id {group_id}!")

        table_by_name = f"{self._GROUP_TABLE_PREFIX}{group_id}"
        if table_by_name in tables:
            return table_by_name

        # Legacy fallback: treat group_id as ordinal table index.
        if group_id < len(tables):
            return tables[group_id]

        raise ValueError(f"Invalid group id {group_id}!")

    def _group_id_from_table_name(self, table_name: str, fallback: int) -> int:
        """Return parsed group id from `sm<id>` table names, else fallback."""
        if table_name.startswith(self._GROUP_TABLE_PREFIX):
            suffix = table_name[len(self._GROUP_TABLE_PREFIX) :]
            if suffix.isdigit():
                return int(suffix)
        return fallback

    @staticmethod
    def __quote_ident(name: str) -> str:
        """Double-quote an identifier to prevent SQL injection via table/column names."""
        return '"' + name.replace('"', '""') + '"'

    @staticmethod
    def __duckdb_type_to_ods(type_str: str) -> ods.DataTypeEnum:
        """Map a DuckDB column type string to an ODS DataTypeEnum value."""
        t = type_str.upper()
        if t == "BOOLEAN":
            return ods.DataTypeEnum.DT_BOOLEAN
        if t == "UTINYINT":
            return ods.DataTypeEnum.DT_BYTE
        if t in ("TINYINT", "SMALLINT"):
            return ods.DataTypeEnum.DT_SHORT
        if t in ("USMALLINT", "INTEGER"):
            return ods.DataTypeEnum.DT_LONG
        if t in ("UINTEGER", "BIGINT"):
            return ods.DataTypeEnum.DT_LONGLONG
        if t in ("UBIGINT", "HUGEINT", "UHUGEINT"):
            return ods.DataTypeEnum.DT_DOUBLE
        if t == "FLOAT":
            return ods.DataTypeEnum.DT_FLOAT
        if t == "DOUBLE" or t.startswith("DECIMAL") or t.startswith("NUMERIC"):
            return ods.DataTypeEnum.DT_DOUBLE
        if t in ("VARCHAR", "TEXT", "STRING"):
            return ods.DataTypeEnum.DT_STRING
        if t == "BLOB":
            return ods.DataTypeEnum.DT_BYTESTR
        if t.startswith("TIMESTAMP") or t in ("DATE", "TIME"):
            return ods.DataTypeEnum.DT_DATE
        if t.startswith("STRUCT("):
            # Complex number convention: STRUCT(re FLOAT, im FLOAT) or STRUCT(re DOUBLE, im DOUBLE)
            if "RE FLOAT" in t or "RE REAL" in t:
                return ods.DataTypeEnum.DT_COMPLEX
            if "RE DOUBLE" in t:
                return ods.DataTypeEnum.DT_DCOMPLEX
        return ods.DataTypeEnum.DT_UNKNOWN


def main() -> None:
    """Run plugin server entry point."""
    from ods_exd_api_box import serve_plugin

    serve_plugin(
        file_type_name="DUCKDB",
        file_type_factory=ExternalDataFile.create,
        file_type_file_patterns=["*.duckdb"],
    )


if __name__ == "__main__":
    main()
