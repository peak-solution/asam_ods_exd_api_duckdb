import logging
import os
import tempfile
import unittest
from pathlib import Path

import duckdb
from google.protobuf.json_format import MessageToJson
from ods_exd_api_box import ExternalDataReader, FileHandlerRegistry, exd_api, ods

from external_data_file import ExternalDataFile

# pylint: disable=E1101


class TestStringMethods(unittest.TestCase):
    log = logging.getLogger(__name__)

    def setUp(self):
        """Register ExternalDataFile handler before each test."""
        FileHandlerRegistry.register(file_type_name="test", factory=ExternalDataFile)

    def _make_simple_file(self, path: str) -> None:
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
            idx = [0.0, 1.0, 2.0, 3.0]
            con.executemany(
                "INSERT INTO measurements VALUES (?, ?, union_value(DT_DOUBLE := ?), ?)",
                [(0, i, i, 0) for i in idx],
            )
            con.executemany(
                "INSERT INTO measurements VALUES (?, ?, union_value(DT_DOUBLE := ?), ?)",
                [(1, i, v, 0) for i, v in zip(idx, [10.0, 20.0, 30.0, 40.0])],
            )
            con.executemany(
                "INSERT INTO measurements VALUES (?, ?, union_value(DT_LONGLONG := ?), ?)",
                [(2, i, v, 0) for i, v in zip(idx, [1000, 2000, 3000, 4000])],
            )
            con.executemany(
                "INSERT INTO measurements VALUES (?, ?, union_value(DT_STRING := ?), ?)",
                [(3, i, v, 0) for i, v in zip(idx, ["a", "b", "c", "d"])],
            )

    def test_open(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = os.path.join(temp_dir, "simple.duckdb")
            self._make_simple_file(file_path)
            service = ExternalDataReader()
            handle = service.Open(exd_api.Identifier(url=Path(file_path).resolve().as_uri(), parameters=""), None)
            try:
                pass
            finally:
                service.Close(handle, None)

    def test_structure(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = os.path.join(temp_dir, "simple.duckdb")
            self._make_simple_file(file_path)
            service = ExternalDataReader()
            handle = service.Open(exd_api.Identifier(url=Path(file_path).resolve().as_uri(), parameters=""), None)
            try:
                structure = service.GetStructure(exd_api.StructureRequest(handle=handle), None)
                self.assertEqual(structure.name, "simple.duckdb")
                self.assertEqual(len(structure.groups), 1)
                self.assertEqual(structure.groups[0].number_of_rows, 4)
                self.assertEqual(len(structure.groups[0].channels), 4)
                self.assertEqual(structure.groups[0].id, 0)
                self.log.info(MessageToJson(structure))
                self.assertEqual(structure.groups[0].channels[0].id, 0)
            finally:
                service.Close(handle, None)

    def test_get_values(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = os.path.join(temp_dir, "simple.duckdb")
            self._make_simple_file(file_path)
            service = ExternalDataReader()
            handle = service.Open(exd_api.Identifier(url=Path(file_path).resolve().as_uri(), parameters=""), None)
            try:
                values = service.GetValues(
                    exd_api.ValuesRequest(handle=handle, group_id=0, channel_ids=[0, 1, 2, 3], start=0, limit=4),
                    None,
                )
                self.assertEqual(values.id, 0)
                self.assertEqual(len(values.channels), 4)
                self.assertEqual(values.channels[0].id, 0)
                self.assertEqual(values.channels[1].id, 1)
                self.log.info(MessageToJson(values))

                self.assertEqual(values.channels[0].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertSequenceEqual(values.channels[0].values.double_array.values, [0.0, 1.0, 2.0, 3.0])
                self.assertEqual(values.channels[1].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertSequenceEqual(values.channels[1].values.double_array.values, [10.0, 20.0, 30.0, 40.0])
                self.assertEqual(values.channels[2].values.data_type, ods.DataTypeEnum.DT_LONGLONG)
                self.assertSequenceEqual(values.channels[2].values.longlong_array.values, [1000, 2000, 3000, 4000])
                self.assertEqual(values.channels[3].values.data_type, ods.DataTypeEnum.DT_STRING)
                self.assertSequenceEqual(values.channels[3].values.string_array.values, ["a", "b", "c", "d"])
            finally:
                service.Close(handle, None)
