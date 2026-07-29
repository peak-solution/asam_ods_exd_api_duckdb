import os
import tempfile
import unittest
from pathlib import Path

import duckdb
import grpc
from google.protobuf.json_format import MessageToJson
from ods_exd_api_box import ExternalDataReader, FileHandlerRegistry, exd_api

from external_data_file import ExternalDataFile
from tests.mock_servicer_context import MockServicerContext

# pylint: disable=E1101


def _make_empty_duckdb(path: str) -> None:
    """Create an empty DuckDB file (no tables)."""
    with duckdb.connect(path):
        pass


def _make_simple_duckdb(path: str) -> None:
    """Create a DuckDB file with one group and two channels."""
    with duckdb.connect(path) as con:
        con.execute(
            "CREATE TABLE measurements ("
            "channel_id BIGINT, "
            '"index" DOUBLE, '
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
            ")"
        )
        con.executemany(
            "INSERT INTO measurements VALUES (?, ?, union_value(DT_DOUBLE := ?), ?)",
            [(0, 0.0, 0.0, 0), (0, 1.0, 1.0, 0)],
        )
        con.executemany(
            "INSERT INTO measurements VALUES (?, ?, union_value(DT_LONG := ?), ?)",
            [(1, 0.0, 1, 0), (1, 1.0, 2, 0)],
        )


def _make_date_duckdb(path: str) -> None:
    """Create a DuckDB file with one DT_DATE channel using ISO timestamp strings."""
    with duckdb.connect(path) as con:
        con.execute(
            "CREATE TABLE measurements ("
            "channel_id BIGINT, "
            '"index" DOUBLE, '
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
            ")"
        )
        con.executemany(
            "INSERT INTO measurements VALUES (?, ?, union_value(DT_DATE := ?), ?)",
            [
                (0, 0.0, "1970-01-01T00:00:00Z", 0),
                (0, 1.0, "2026-01-02T03:04:05.123456Z", 0),
            ],
        )


class TestExternalDataReader(unittest.TestCase):
    def setUp(self):
        FileHandlerRegistry.register(file_type_name="test", factory=ExternalDataFile)
        self.service = ExternalDataReader()
        self.mock_context = MockServicerContext()

    def test_open_existing_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = os.path.join(temp_dir, "test.duckdb")
            _make_empty_duckdb(file_path)

            identifier = exd_api.Identifier(url=Path(file_path).resolve().as_uri(), parameters="")
            try:
                handle = self.service.Open(identifier, None)
                self.assertIsNotNone(handle.uuid)
            finally:
                self.service.Close(handle, None)

    def test_open_non_existing_file(self):
        identifier = exd_api.Identifier(url="file:///non_existing_file.duckdb", parameters="")
        with self.assertRaises(grpc.RpcError) as _:
            self.service.Open(identifier, self.mock_context)

        self.assertEqual(self.mock_context.code(), grpc.StatusCode.NOT_FOUND)

    def test_simple_example(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = os.path.join(temp_dir, "simple.duckdb")
            _make_simple_duckdb(file_path)
            main_file_url = Path(file_path).absolute().resolve().as_uri()

            main_external_data_reader = ExternalDataReader()
            main_exd_api_handle = main_external_data_reader.Open(
                exd_api.Identifier(url=main_file_url, parameters=""), None
            )
            try:
                main_exd_api_structure = main_external_data_reader.GetStructure(
                    exd_api.StructureRequest(handle=main_exd_api_handle), None
                )
                print(MessageToJson(main_exd_api_structure))

                print(
                    MessageToJson(
                        main_external_data_reader.GetValues(
                            exd_api.ValuesRequest(
                                handle=main_exd_api_handle,
                                group_id=0,
                                channel_ids=[0, 1],
                                start=0,
                                limit=10,
                            ),
                            None,
                        )
                    )
                )
            finally:
                main_external_data_reader.Close(main_exd_api_handle, None)

    def test_close_duckdb(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = os.path.join(temp_dir, "test.duckdb")
            _make_empty_duckdb(file_path)

            identifier = exd_api.Identifier(url=Path(file_path).resolve().as_uri(), parameters="")
            handle1 = self.service.Open(identifier, None)
            self.assertIsNotNone(handle1.uuid)

            handle2 = self.service.Open(identifier, None)
            self.assertIsNotNone(handle2.uuid)

            self.assertNotEqual(handle1.uuid, handle2.uuid)

            self.service.Close(handle1, None)
            self.service.Close(handle2, None)

    def test_dt_date_values_are_returned_in_ods_format(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = os.path.join(temp_dir, "date_test.duckdb")
            _make_date_duckdb(file_path)

            identifier = exd_api.Identifier(url=Path(file_path).resolve().as_uri(), parameters="")
            handle = self.service.Open(identifier, None)
            try:
                values = self.service.GetValues(
                    exd_api.ValuesRequest(handle=handle, group_id=0, channel_ids=[0], start=0, limit=10),
                    None,
                )
                self.assertSequenceEqual(
                    list(values.channels[0].values.string_array.values),
                    ["19700101000000000000000", "20260102030405123456000"],
                )
            finally:
                self.service.Close(handle, None)

    def test_duckdb_file_has_start_time(self):
        """start_time attribute must be present and non-empty (uses datetime.now())."""
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = os.path.join(temp_dir, "meta.duckdb")
            _make_empty_duckdb(file_path)

            identifier = exd_api.Identifier(url=Path(file_path).resolve().as_uri(), parameters="")
            handle = self.service.Open(identifier, None)
            try:
                structure = self.service.GetStructure(exd_api.StructureRequest(handle=handle), None)
                attribute = structure.attributes.variables.get("start_time")
                self.assertIsNotNone(attribute)
                self.assertGreater(len(attribute.string_array.values[0]), 0)
            finally:
                self.service.Close(handle, None)
