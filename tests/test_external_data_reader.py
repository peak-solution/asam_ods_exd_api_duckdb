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
    """Create a DuckDB file with one table and two columns."""
    with duckdb.connect(path) as con:
        con.execute('CREATE TABLE measurements ("index" DOUBLE, value INTEGER)')
        con.executemany('INSERT INTO measurements VALUES (?, ?)', [(0.0, 1), (1.0, 2)])


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
        main_file_path = Path.joinpath(Path(__file__).parent.resolve(), "..", "data", "simple.duckdb")
        main_file_url = Path(main_file_path).absolute().resolve().as_uri()

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
                            channel_ids=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
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
