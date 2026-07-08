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


def _make_all_datatypes_duckdb(path: str) -> None:
    """Create a DuckDB file with 4 tables covering supported ODS types."""
    with duckdb.connect(path) as con:
        schema = (
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
        )
        con.execute(f"CREATE TABLE group_complex ({schema})")
        con.execute(f"CREATE TABLE group_int ({schema})")
        con.execute(f"CREATE TABLE group_real ({schema})")
        con.execute(f"CREATE TABLE group_string ({schema})")

        idx = [0.0, 1.0]
        con.executemany(
            "INSERT INTO group_complex VALUES (?, ?, union_value(DT_DOUBLE := ?), ?)",
            [(0, i, i, 0) for i in idx],
        )
        con.executemany(
            "INSERT INTO group_complex VALUES (?, ?, union_value(DT_COMPLEX := ?), ?)",
            [
                (1, 0.0, {"re": 1.0, "im": 2.0}, 0),
                (1, 1.0, {"re": 3.0, "im": 4.0}, 0),
            ],
        )
        con.executemany(
            "INSERT INTO group_complex VALUES (?, ?, union_value(DT_DCOMPLEX := ?), ?)",
            [
                (2, 0.0, {"re": 5.0, "im": 6.0}, 0),
                (2, 1.0, {"re": 7.0, "im": 8.0}, 0),
            ],
        )

        con.executemany(
            "INSERT INTO group_int VALUES (?, ?, union_value(DT_DOUBLE := ?), ?)",
            [(0, i, i, 0) for i in idx],
        )
        con.executemany(
            "INSERT INTO group_int VALUES (?, ?, union_value(DT_SHORT := ?), ?)",
            [(1, 0.0, -2, 0), (1, 1.0, 4, 0)],
        )
        con.executemany(
            "INSERT INTO group_int VALUES (?, ?, union_value(DT_BYTE := ?), ?)",
            [(2, 0.0, 2, 0), (2, 1.0, 4, 0)],
        )
        con.executemany(
            "INSERT INTO group_int VALUES (?, ?, union_value(DT_SHORT := ?), ?)",
            [(3, 0.0, -2, 0), (3, 1.0, 4, 0)],
        )
        con.executemany(
            "INSERT INTO group_int VALUES (?, ?, union_value(DT_LONG := ?), ?)",
            [(4, 0.0, 2, 0), (4, 1.0, 4, 0)],
        )
        con.executemany(
            "INSERT INTO group_int VALUES (?, ?, union_value(DT_LONG := ?), ?)",
            [(5, 0.0, -2, 0), (5, 1.0, 4, 0)],
        )
        con.executemany(
            "INSERT INTO group_int VALUES (?, ?, union_value(DT_LONGLONG := ?), ?)",
            [(6, 0.0, 2, 0), (6, 1.0, 4, 0)],
        )
        con.executemany(
            "INSERT INTO group_int VALUES (?, ?, union_value(DT_LONGLONG := ?), ?)",
            [(7, 0.0, -2, 0), (7, 1.0, 4, 0)],
        )
        con.executemany(
            "INSERT INTO group_int VALUES (?, ?, union_value(DT_DOUBLE := ?), ?)",
            [(8, 0.0, 2.0, 0), (8, 1.0, 4.0, 0)],
        )

        con.executemany(
            "INSERT INTO group_real VALUES (?, ?, union_value(DT_DOUBLE := ?), ?)",
            [(0, i, i, 0) for i in idx],
        )
        con.executemany(
            "INSERT INTO group_real VALUES (?, ?, union_value(DT_FLOAT := ?), ?)",
            [(1, 0.0, 1.1, 0), (1, 1.0, 1.2, 0)],
        )
        con.executemany(
            "INSERT INTO group_real VALUES (?, ?, union_value(DT_DOUBLE := ?), ?)",
            [(2, 0.0, 2.1, 0), (2, 1.0, 2.2, 0)],
        )

        con.executemany(
            "INSERT INTO group_string VALUES (?, ?, union_value(DT_DOUBLE := ?), ?)",
            [(0, i, i, 0) for i in idx],
        )
        con.executemany(
            "INSERT INTO group_string VALUES (?, ?, union_value(DT_STRING := ?), ?)",
            [(1, 0.0, "abc", 0), (1, 1.0, "def", 0)],
        )


class TestDataTypes(unittest.TestCase):
    log = logging.getLogger(__name__)

    def setUp(self):
        FileHandlerRegistry.register(file_type_name="test", factory=ExternalDataFile)

    def test_data_type(self):
        with tempfile.TemporaryDirectory() as temporary_directory_name:
            file_path = os.path.join(temporary_directory_name, "all_datatypes_test.duckdb")
            _make_all_datatypes_duckdb(file_path)

            service = ExternalDataReader()
            handle = service.Open(exd_api.Identifier(url=Path(file_path).resolve().as_uri(), parameters=""), None)
            try:
                structure = service.GetStructure(exd_api.StructureRequest(handle=handle), None)
                self.log.info(MessageToJson(structure))

                self.assertEqual(structure.name, "all_datatypes_test.duckdb")
                self.assertEqual(len(structure.groups), 4)
                # Tables sorted alphabetically: group_complex, group_int, group_real, group_string
                self.assertEqual(structure.groups[0].number_of_rows, 2)
                self.assertEqual(len(structure.groups[0].channels), 3)
                self.assertEqual(structure.groups[1].number_of_rows, 2)
                self.assertEqual(len(structure.groups[1].channels), 9)
                self.assertEqual(structure.groups[2].number_of_rows, 2)
                self.assertEqual(len(structure.groups[2].channels), 3)
                self.assertEqual(structure.groups[3].number_of_rows, 2)
                self.assertEqual(len(structure.groups[3].channels), 2)

                self.assertEqual(structure.groups[0].channels[1].data_type, ods.DataTypeEnum.DT_COMPLEX)
                self.assertEqual(structure.groups[0].channels[2].data_type, ods.DataTypeEnum.DT_DCOMPLEX)

                self.assertEqual(structure.groups[1].channels[1].data_type, ods.DataTypeEnum.DT_SHORT)
                self.assertEqual(structure.groups[1].channels[2].data_type, ods.DataTypeEnum.DT_BYTE)
                self.assertEqual(structure.groups[1].channels[3].data_type, ods.DataTypeEnum.DT_SHORT)
                self.assertEqual(structure.groups[1].channels[4].data_type, ods.DataTypeEnum.DT_LONG)
                self.assertEqual(structure.groups[1].channels[5].data_type, ods.DataTypeEnum.DT_LONG)
                self.assertEqual(structure.groups[1].channels[6].data_type, ods.DataTypeEnum.DT_LONGLONG)
                self.assertEqual(structure.groups[1].channels[7].data_type, ods.DataTypeEnum.DT_LONGLONG)
                self.assertEqual(structure.groups[1].channels[8].data_type, ods.DataTypeEnum.DT_DOUBLE)

                self.assertEqual(structure.groups[2].channels[1].data_type, ods.DataTypeEnum.DT_FLOAT)
                self.assertEqual(structure.groups[2].channels[2].data_type, ods.DataTypeEnum.DT_DOUBLE)

                self.assertEqual(structure.groups[3].channels[1].data_type, ods.DataTypeEnum.DT_STRING)

                values = service.GetValues(
                    exd_api.ValuesRequest(handle=handle, group_id=0, start=0, limit=2, channel_ids=[0, 1, 2]), None
                )
                self.assertEqual(values.channels[1].values.data_type, ods.DataTypeEnum.DT_COMPLEX)
                self.assertSequenceEqual(values.channels[1].values.float_array.values, [1.0, 2.0, 3.0, 4.0])
                self.assertEqual(values.channels[2].values.data_type, ods.DataTypeEnum.DT_DCOMPLEX)
                self.assertSequenceEqual(values.channels[2].values.double_array.values, [5.0, 6.0, 7.0, 8.0])

                values = service.GetValues(
                    exd_api.ValuesRequest(
                        handle=handle, group_id=1, start=0, limit=2, channel_ids=[0, 1, 2, 3, 4, 5, 6, 7, 8]
                    ),
                    None,
                )
                self.assertEqual(values.channels[1].values.data_type, ods.DataTypeEnum.DT_SHORT)
                self.assertSequenceEqual(values.channels[1].values.long_array.values, [-2, 4])
                self.assertEqual(values.channels[2].values.data_type, ods.DataTypeEnum.DT_BYTE)
                self.assertSequenceEqual(list(values.channels[2].values.byte_array.values), [2, 4])
                self.assertEqual(values.channels[3].values.data_type, ods.DataTypeEnum.DT_SHORT)
                self.assertSequenceEqual(values.channels[3].values.long_array.values, [-2, 4])
                self.assertEqual(values.channels[4].values.data_type, ods.DataTypeEnum.DT_LONG)
                self.assertSequenceEqual(values.channels[4].values.long_array.values, [2, 4])
                self.assertEqual(values.channels[5].values.data_type, ods.DataTypeEnum.DT_LONG)
                self.assertSequenceEqual(values.channels[5].values.long_array.values, [-2, 4])
                self.assertEqual(values.channels[6].values.data_type, ods.DataTypeEnum.DT_LONGLONG)
                self.assertSequenceEqual(values.channels[6].values.longlong_array.values, [2, 4])
                self.assertEqual(values.channels[7].values.data_type, ods.DataTypeEnum.DT_LONGLONG)
                self.assertSequenceEqual(values.channels[7].values.longlong_array.values, [-2, 4])
                self.assertEqual(values.channels[8].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertSequenceEqual(values.channels[8].values.double_array.values, [2.0, 4.0])

                values = service.GetValues(
                    exd_api.ValuesRequest(handle=handle, group_id=2, start=0, limit=2, channel_ids=[0, 1, 2]), None
                )
                self.assertEqual(values.channels[1].values.data_type, ods.DataTypeEnum.DT_FLOAT)
                self.assertAlmostEqual(values.channels[1].values.float_array.values[0], 1.1, places=5)
                self.assertAlmostEqual(values.channels[1].values.float_array.values[1], 1.2, places=5)
                self.assertEqual(values.channels[2].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertSequenceEqual(values.channels[2].values.double_array.values, [2.1, 2.2])

                values = service.GetValues(
                    exd_api.ValuesRequest(handle=handle, group_id=3, start=0, limit=2, channel_ids=[0, 1]), None
                )
                self.assertEqual(values.channels[1].values.data_type, ods.DataTypeEnum.DT_STRING)
                self.assertSequenceEqual(values.channels[1].values.string_array.values, ["abc", "def"])

            finally:
                service.Close(handle, None)

    def test_independent(self):
        with tempfile.TemporaryDirectory() as temporary_directory_name:
            file_path = os.path.join(temporary_directory_name, "independent_test.duckdb")

            with duckdb.connect(file_path) as con:
                con.execute(
                    "CREATE TABLE group0 ("
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
                    "INSERT INTO group0 VALUES (?, ?, union_value(DT_DOUBLE := ?), ?)",
                    [(0, 0.0, 0.0, 0), (0, 1.0, 1.0, 0)],
                )
                con.executemany(
                    "INSERT INTO group0 VALUES (?, ?, union_value(DT_LONG := ?), ?)",
                    [(1, 0.0, 2, 0), (1, 1.0, 4, 0)],
                )

            service = ExternalDataReader()
            handle = service.Open(exd_api.Identifier(url=Path(file_path).resolve().as_uri(), parameters=""), None)
            try:
                structure = service.GetStructure(exd_api.StructureRequest(handle=handle), None)
                self.log.info(MessageToJson(structure))

                self.assertEqual(structure.name, "independent_test.duckdb")
                self.assertEqual(len(structure.groups), 1)
                self.assertEqual(structure.groups[0].number_of_rows, 2)
                self.assertEqual(len(structure.groups[0].channels), 2)

                self.assertEqual("0", structure.groups[0].channels[0].name)
                self.assertEqual("1", structure.groups[0].channels[1].name)

                self.assertEqual(structure.groups[0].channels[0].data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertEqual(structure.groups[0].channels[1].data_type, ods.DataTypeEnum.DT_LONG)

                self.assertIn("independent", structure.groups[0].channels[0].attributes.variables)
                self.assertEqual(
                    1, structure.groups[0].channels[0].attributes.variables["independent"].long_array.values[0]
                )
                self.assertNotIn("independent", structure.groups[0].channels[1].attributes.variables)

                values = service.GetValues(
                    exd_api.ValuesRequest(handle=handle, group_id=0, start=0, limit=2, channel_ids=[0, 1]), None
                )
                self.assertEqual(values.channels[0].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertSequenceEqual(values.channels[0].values.double_array.values, [0.0, 1.0])
                self.assertEqual(values.channels[1].values.data_type, ods.DataTypeEnum.DT_LONG)
                self.assertSequenceEqual(values.channels[1].values.long_array.values, [2, 4])
            finally:
                service.Close(handle, None)
