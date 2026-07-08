"""ASAM ODS EXD API implementation for DuckDB files."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import override

import duckdb
from ods_exd_api_box import ExdFileInterface, exd_api, ods


class ExternalDataFile(ExdFileInterface):
    """Class for handling DuckDB files."""

    _INDEPENDENT_COLUMN = "index"

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
        self._log.debug("Building structure for file: %s", self.file_path)

        start_time_ods = datetime.now().strftime("%Y%m%d%H%M%S%f")
        structure.attributes.variables["start_time"].string_array.values.append(start_time_ods)

        tables = self._list_tables()
        self._log.debug("Found %d tables in DuckDB", len(tables))

        for group_index, table_name in enumerate(tables):
            columns = self._list_columns(table_name)
            row_count = self.con.execute(
                f"SELECT COUNT(*) FROM {self.__quote_ident(table_name)}"
            ).fetchone()[0]

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

    @override
    def get_values(self, request: exd_api.ValuesRequest) -> exd_api.ValuesResult:
        """Get values from the external data file."""
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

        nr_of_rows = self.con.execute(f"SELECT COUNT(*) FROM {quoted_table}").fetchone()[0]
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
                new_channel_values.values.byte_array.values = bytes(section)
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
                raise NotImplementedError(
                    f"Unsupported DuckDB type {col_type!r} for column {col_name!r}"
                )

            rv.channels.append(new_channel_values)

        self._log.debug("GetValues returning %d channels", len(rv.channels))
        return rv

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

    serve_plugin(file_type_name="DUCKDB", file_type_factory=ExternalDataFile.create, file_type_file_patterns=["*.duckdb"])


if __name__ == "__main__":
    main()
