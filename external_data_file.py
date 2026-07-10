"""ASAM ODS EXD API implementation for DuckDB files."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import override

import duckdb
from ods_exd_api_box import ExdFileInterface, exd_api, ods


class ExternalDataFile(ExdFileInterface):
    """Class for handling DuckDB files."""

    _INDEPENDENT_COLUMN = "index"
    _NORMALIZED_COLUMNS = {"name", "index", "value", "flag"}
    _NORMALIZED_MEMBER_TO_ODS = {
        "DT_DOUBLE": ods.DataTypeEnum.DT_DOUBLE,
        "DT_FLOAT": ods.DataTypeEnum.DT_FLOAT,
        "DT_SHORT": ods.DataTypeEnum.DT_SHORT,
        "DT_LONG": ods.DataTypeEnum.DT_LONG,
        "DT_LONGLONG": ods.DataTypeEnum.DT_LONGLONG,
        "DT_STRING": ods.DataTypeEnum.DT_STRING,
    }
    _NORMALIZED_MEMBER_ORDER = [
        "DT_DOUBLE",
        "DT_FLOAT",
        "DT_SHORT",
        "DT_LONG",
        "DT_LONGLONG",
        "DT_STRING",
    ]

    @dataclass(frozen=True)
    class _NormalizedChannel:
        name: str
        union_member: str
        ods_type: ods.DataTypeEnum

    @dataclass(frozen=True)
    class _NormalizedTableMeta:
        index_type: ods.DataTypeEnum
        channels: list[ExternalDataFile._NormalizedChannel]

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
        self._mode_normalized = self._is_normalized_mode(parameters)
        self._normalized_table_meta: dict[str, ExternalDataFile._NormalizedTableMeta] = {}
        self._normalized_union_members: dict[str, dict[str, set[str]]] = {}
        self.con = duckdb.connect(file_path, read_only=True)
        self._log.debug("ExternalDataFile initialized")

    @override
    def close(self) -> None:
        """Close the external data file."""
        self.con.close()
        self._log.debug("ExternalDataFile closed")

    @override
    def fill_structure(self, structure: exd_api.StructureResult) -> None:
        """Fill the structure of the external data file."""
        if self._mode_normalized:
            self._fill_structure_normalized(structure)
            return

        self._fill_structure_classic(structure)

    def _fill_structure_classic(self, structure: exd_api.StructureResult) -> None:
        """Fill structure by mapping one table column to one channel."""
        self._log.debug("Building structure for file: %s", self.file_path)

        start_time_ods = datetime.now().strftime("%Y%m%d%H%M%S%f")
        structure.attributes.variables["start_time"].string_array.values.append(start_time_ods)

        tables = self._list_tables()
        self._log.debug("Found %d tables in DuckDB", len(tables))

        for group_index, table_name in enumerate(tables):
            columns = self._list_columns(table_name)
            row_count_row = self.con.execute(f"SELECT COUNT(*) FROM {self.__quote_ident(table_name)}").fetchone()
            if row_count_row is None:
                raise ValueError(f"Could not read row count for table {table_name!r}")
            row_count = row_count_row[0]

            new_group = exd_api.StructureResult.Group()
            new_group.name = table_name
            new_group.id = group_index
            new_group.total_number_of_channels = len(columns)
            new_group.number_of_rows = row_count

            for channel_index, (col_name, col_type) in enumerate(columns):
                new_channel = exd_api.StructureResult.Channel()
                new_channel.name = col_name
                new_channel.id = channel_index
                new_channel.data_type = self.__duckdb_type_to_ods(col_type)
                new_channel.unit_string = ""
                if col_name == self._INDEPENDENT_COLUMN:
                    new_channel.attributes.variables["independent"].long_array.values.append(1)
                new_group.channels.append(new_channel)

            structure.groups.append(new_group)
            self._log.debug(
                "Added group id=%d name=%s channels=%d rows=%d",
                new_group.id,
                new_group.name,
                len(new_group.channels),
                new_group.number_of_rows,
            )

    def _fill_structure_normalized(self, structure: exd_api.StructureResult) -> None:
        """Fill structure for normalized schema with name/index/value/flag rows."""
        self._log.debug("Building normalized structure for file: %s", self.file_path)

        start_time_ods = datetime.now().strftime("%Y%m%d%H%M%S%f")
        structure.attributes.variables["start_time"].string_array.values.append(start_time_ods)

        tables = self._list_tables()
        for group_index, table_name in enumerate(tables):
            table_meta = self._get_normalized_table_meta(table_name)
            quoted_table = self.__quote_ident(table_name)
            row_count_row = self.con.execute(
                f"SELECT COUNT(DISTINCT {self.__quote_ident(self._INDEPENDENT_COLUMN)}) FROM {quoted_table}"
            ).fetchone()
            if row_count_row is None:
                raise ValueError(f"Could not read row count for table {table_name!r}")
            row_count = row_count_row[0]

            new_group = exd_api.StructureResult.Group()
            new_group.name = table_name
            new_group.id = group_index
            new_group.total_number_of_channels = len(table_meta.channels)
            new_group.number_of_rows = row_count

            for channel_index, channel_meta in enumerate(table_meta.channels):
                new_channel = exd_api.StructureResult.Channel()
                new_channel.name = channel_meta.name
                new_channel.id = channel_index
                new_channel.data_type = channel_meta.ods_type
                new_channel.unit_string = ""
                new_group.channels.append(new_channel)

            structure.groups.append(new_group)

    @override
    def get_values(self, request: exd_api.ValuesRequest) -> exd_api.ValuesResult:
        """Get values from the external data file."""
        if self._mode_normalized:
            return self._get_values_normalized(request)

        return self._get_values_classic(request)

    def _get_values_classic(self, request: exd_api.ValuesRequest) -> exd_api.ValuesResult:
        """Get values for classic column-wise tables."""
        self._log.debug(
            "GetValues request group_id=%d start=%d limit=%d channel_count=%d",
            request.group_id,
            request.start,
            request.limit,
            len(request.channel_ids),
        )

        tables = self._list_tables()
        if request.group_id < 0 or request.group_id >= len(tables):
            raise ValueError(f"Invalid group id {request.group_id}!")

        table_name = tables[request.group_id]
        columns = self._list_columns(table_name)
        quoted_table = self.__quote_ident(table_name)

        nr_of_rows_row = self.con.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()
        if nr_of_rows_row is None:
            raise ValueError(f"Could not read row count for table {table_name!r}")
        nr_of_rows = nr_of_rows_row[0]
        if request.start > nr_of_rows:
            raise ValueError(f"Channel start index {request.start} out of range!")

        rv = exd_api.ValuesResult(id=request.group_id)

        for channel_id in request.channel_ids:
            if channel_id < 0 or channel_id >= len(columns):
                raise ValueError(f"Invalid channel id {channel_id}!")

            col_name, col_type = columns[channel_id]
            ods_type = self.__duckdb_type_to_ods(col_type)
            quoted_col = self.__quote_ident(col_name)

            rows = self.con.execute(
                f"SELECT {quoted_col} FROM {quoted_table} LIMIT ? OFFSET ?",
                [request.limit, request.start],
            ).fetchall()
            section = [row[0] for row in rows]

            new_channel_values = exd_api.ValuesResult.ChannelValues()
            new_channel_values.id = channel_id
            new_channel_values.values.data_type = ods_type

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
                    interleaved.append(float(v["re"]))
                    interleaved.append(float(v["im"]))
                new_channel_values.values.float_array.values[:] = interleaved
            elif ods_type == ods.DataTypeEnum.DT_DCOMPLEX:
                interleaved_d: list[float] = []
                for v in section:
                    interleaved_d.append(float(v["re"]))
                    interleaved_d.append(float(v["im"]))
                new_channel_values.values.double_array.values[:] = interleaved_d
            elif ods_type == ods.DataTypeEnum.DT_STRING:
                new_channel_values.values.string_array.values[:] = section
            elif ods_type == ods.DataTypeEnum.DT_BYTESTR:
                for item in section:
                    new_channel_values.values.bytestr_array.values.append(
                        item if isinstance(item, bytes) else bytes(item)
                    )
            elif ods_type == ods.DataTypeEnum.DT_DATE:
                new_channel_values.values.string_array.values[:] = [str(v) for v in section]
            else:
                raise NotImplementedError(f"Unsupported DuckDB type {col_type!r} for column {col_name!r}")

            rv.channels.append(new_channel_values)

        self._log.debug("GetValues returning %d channels", len(rv.channels))
        return rv

    def _get_values_normalized(self, request: exd_api.ValuesRequest) -> exd_api.ValuesResult:
        """Get values for normalized schema where channel samples are rows keyed by name."""
        self._log.debug(
            "GetValues(normalized) request group_id=%d start=%d limit=%d channel_count=%d",
            request.group_id,
            request.start,
            request.limit,
            len(request.channel_ids),
        )

        tables = self._list_tables()
        if request.group_id < 0 or request.group_id >= len(tables):
            raise ValueError(f"Invalid group id {request.group_id}!")

        table_name = tables[request.group_id]
        table_meta = self._get_normalized_table_meta(table_name)
        quoted_table = self.__quote_ident(table_name)
        quoted_index = self.__quote_ident(self._INDEPENDENT_COLUMN)

        nr_of_rows_row = self.con.execute(f"SELECT COUNT(DISTINCT {quoted_index}) FROM {quoted_table}").fetchone()
        if nr_of_rows_row is None:
            raise ValueError(f"Could not read row count for table {table_name!r}")
        nr_of_rows = nr_of_rows_row[0]
        if request.start > nr_of_rows:
            raise ValueError(f"Channel start index {request.start} out of range!")

        rv = exd_api.ValuesResult(id=request.group_id)
        max_channel_id = len(table_meta.channels) - 1

        for channel_id in request.channel_ids:
            if channel_id < 0 or channel_id > max_channel_id:
                raise ValueError(f"Invalid channel id {channel_id}!")

            channel_meta = table_meta.channels[channel_id]
            ods_type = channel_meta.ods_type
            rows = self.con.execute(
                f"SELECT {self.__quote_ident('value')} FROM {quoted_table} "
                f"WHERE {self.__quote_ident('name')} = ? "
                f"ORDER BY {quoted_index} LIMIT ? OFFSET ?",
                [channel_meta.name, request.limit, request.start],
            ).fetchall()
            section = [self._extract_normalized_value(row[0], channel_meta.union_member) for row in rows]

            new_channel_values = exd_api.ValuesResult.ChannelValues()
            new_channel_values.id = channel_id
            new_channel_values.values.data_type = ods_type

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
            elif ods_type == ods.DataTypeEnum.DT_STRING:
                new_channel_values.values.string_array.values[:] = section
            else:
                raise NotImplementedError(f"Unsupported normalized ODS type {ods_type!r}")

            rv.channels.append(new_channel_values)

        return rv

    def _get_normalized_table_meta(self, table_name: str) -> _NormalizedTableMeta:
        """Build and cache normalized channel mapping for one table."""
        if table_name in self._normalized_table_meta:
            return self._normalized_table_meta[table_name]

        columns = self._list_columns(table_name)
        column_names = {name for name, _ in columns}
        if column_names != self._NORMALIZED_COLUMNS:
            raise ValueError(
                f"Normalized mode requires columns {sorted(self._NORMALIZED_COLUMNS)!r}, "
                f"got {sorted(column_names)!r} for table {table_name!r}."
            )

        index_type_str = next(col_type for col_name, col_type in columns if col_name == self._INDEPENDENT_COLUMN)
        index_type = self.__duckdb_type_to_ods(index_type_str)

        quoted_table = self.__quote_ident(table_name)
        distinct_names = self.con.execute(
            f"SELECT DISTINCT {self.__quote_ident('name')} FROM {quoted_table} ORDER BY 1"
        ).fetchall()

        channels: list[ExternalDataFile._NormalizedChannel] = []
        for row in distinct_names:
            channel_name = row[0]
            if channel_name == self._INDEPENDENT_COLUMN:
                continue
            union_member = self._detect_channel_union_member(table_name, channel_name)
            if union_member not in self._NORMALIZED_MEMBER_TO_ODS:
                raise ValueError(f"Unsupported normalized union member {union_member!r} for channel {channel_name!r}")

            channels.append(
                ExternalDataFile._NormalizedChannel(
                    name=channel_name,
                    union_member=union_member,
                    ods_type=self._NORMALIZED_MEMBER_TO_ODS[union_member],
                )
            )

        table_meta = ExternalDataFile._NormalizedTableMeta(index_type=index_type, channels=channels)
        self._normalized_table_meta[table_name] = table_meta
        return table_meta

    def _detect_channel_union_member(self, table_name: str, channel_name: str) -> str:
        """Resolve the active UNION member for one channel name."""
        quoted_table = self.__quote_ident(table_name)
        quoted_name = self.__quote_ident("name")
        quoted_value = self.__quote_ident("value")

        if table_name not in self._normalized_union_members:
            try:
                rows = self.con.execute(
                    f"SELECT {quoted_name}, CAST(union_tag({quoted_value}) AS VARCHAR) "
                    f"FROM {quoted_table} GROUP BY {quoted_name}, union_tag({quoted_value})"
                ).fetchall()
            except duckdb.Error:
                rows = []

            members_by_name: dict[str, set[str]] = {}
            for name_value, member_value in rows:
                if name_value is None or member_value is None:
                    continue
                channel_members = members_by_name.setdefault(str(name_value), set())
                channel_members.add(str(member_value).upper())
            self._normalized_union_members[table_name] = members_by_name

        members = sorted(self._normalized_union_members.get(table_name, {}).get(channel_name, set()))
        if not members:
            sample = self.con.execute(
                f"SELECT {quoted_value} FROM {quoted_table} "
                f"WHERE {quoted_name} = ? AND {quoted_value} IS NOT NULL LIMIT 1",
                [channel_name],
            ).fetchone()
            if sample is None:
                raise ValueError(f"Could not infer type for normalized channel {channel_name!r}")
            return self._detect_union_member(sample[0])
        if len(members) > 1:
            raise ValueError(f"Normalized channel {channel_name!r} uses mixed union members, which is not supported.")
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

        direct_members = [name for name in self._NORMALIZED_MEMBER_ORDER if name in value and value[name] is not None]
        if len(direct_members) == 1:
            return direct_members[0]

        tag_value = value.get("__tag")
        if isinstance(tag_value, str):
            return tag_value
        if isinstance(tag_value, int) and 0 <= tag_value < len(self._NORMALIZED_MEMBER_ORDER):
            return self._NORMALIZED_MEMBER_ORDER[tag_value]

        raise ValueError(f"Could not determine union member from value {value!r}")

    def _extract_normalized_value(self, value: object, member: str) -> object:
        """Extract active value payload from a DuckDB UNION dictionary."""
        if value is None:
            raise ValueError("NULL normalized values are not supported")
        if not isinstance(value, dict):
            return value
        if member not in value:
            raise ValueError(f"Union member {member!r} missing from value {value!r}")
        return value[member]

    @staticmethod
    def _is_normalized_mode(parameters: str) -> bool:
        """Return True when identifier parameters request normalized mode."""
        for token in parameters.replace(";", ",").split(","):
            part = token.strip()
            if not part:
                continue
            if "=" not in part:
                continue
            key, value = part.split("=", maxsplit=1)
            if key.strip().lower() == "mode" and value.strip().lower() == "normalized":
                return True
        return False

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
