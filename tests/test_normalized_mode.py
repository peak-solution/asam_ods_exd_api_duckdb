import os
import tempfile
import unittest
from pathlib import Path
from typing import cast

import duckdb
from ods_exd_api_box import ExternalDataReader, FileHandlerRegistry, exd_api, ods

from external_data_file import ExternalDataFile

# pylint: disable=E1101


class TestNormalizedMode(unittest.TestCase):
    def setUp(self):
        FileHandlerRegistry.register(file_type_name="test", factory=ExternalDataFile)
        self.service = ExternalDataReader()
        self.tmp = tempfile.TemporaryDirectory()
        self.tmp_dir = self.tmp.name

    def tearDown(self):
        self.tmp.cleanup()

    @staticmethod
    def _uri(path: str) -> str:
        return Path(path).resolve().as_uri()

    def _open(self, path: str, parameters: str = ""):
        return self.service.Open(exd_api.Identifier(url=self._uri(path), parameters=parameters), None)

    def _structure(self, handle) -> exd_api.StructureResult:
        return cast(
            exd_api.StructureResult,
            self.service.GetStructure(exd_api.StructureRequest(handle=handle), None),
        )

    def _values(self, handle, group_id, channel_ids, start=0, limit=9999) -> exd_api.ValuesResult:
        return cast(
            exd_api.ValuesResult,
            self.service.GetValues(
                exd_api.ValuesRequest(
                    handle=handle,
                    group_id=group_id,
                    channel_ids=channel_ids,
                    start=start,
                    limit=limit,
                ),
                None,
            ),
        )

    def _make_file(self, name: str) -> str:
        path = os.path.join(self.tmp_dir, name)
        with duckdb.connect(path) as con:
            con.execute(
                "CREATE TABLE mea ("
                "channel_id BIGINT, "
                '"index" BIGINT, '
                "value UNION("
                "DT_DOUBLE DOUBLE, "
                "DT_FLOAT FLOAT, "
                "DT_SHORT SHORT, "
                "DT_LONG INT, "
                "DT_LONGLONG LONG, "
                "DT_STRING VARCHAR"
                "), "
                "flag SHORT"
                ")"
            )
            con.executemany(
                "INSERT INTO mea VALUES (?, ?, union_value(DT_DOUBLE := ?), ?)",
                [
                    (2, 0, 1.5, 0),
                    (2, 1, 2.5, 0),
                    (2, 2, 3.5, 0),
                ],
            )
            con.executemany(
                "INSERT INTO mea VALUES (?, ?, union_value(DT_LONG := ?), ?)",
                [
                    (1, 0, 1000, 0),
                    (1, 1, 1100, 0),
                    (1, 2, 1200, 0),
                ],
            )
            con.executemany(
                "INSERT INTO mea VALUES (?, ?, union_value(DT_STRING := ?), ?)",
                [
                    (3, 0, "idle", 0),
                    (3, 1, "run", 0),
                    (3, 2, "run", 0),
                ],
            )
        return path

    def test_structure_and_values(self):
        path = self._make_file("normalized.duckdb")
        handle = self._open(path)
        try:
            structure = self._structure(handle)
            self.assertEqual(len(structure.groups), 1)
            group = structure.groups[0]
            self.assertEqual(group.name, "mea")
            self.assertEqual(group.number_of_rows, 3)
            self.assertEqual(group.total_number_of_channels, 3)

            # channels are keyed by numeric channel_id
            self.assertSequenceEqual([ch.id for ch in group.channels], [1, 2, 3])
            self.assertSequenceEqual([ch.name for ch in group.channels], ["1", "2", "3"])
            self.assertEqual(group.channels[0].data_type, ods.DataTypeEnum.DT_LONG)
            self.assertEqual(group.channels[1].data_type, ods.DataTypeEnum.DT_DOUBLE)
            self.assertEqual(group.channels[2].data_type, ods.DataTypeEnum.DT_STRING)

            values = self._values(handle, 0, [1, 2, 3], start=0, limit=3)
            self.assertSequenceEqual(list(values.channels[0].values.long_array.values), [1000, 1100, 1200])
            self.assertSequenceEqual(list(values.channels[1].values.double_array.values), [1.5, 2.5, 3.5])
            self.assertSequenceEqual(list(values.channels[2].values.string_array.values), ["idle", "run", "run"])
        finally:
            self.service.Close(handle, None)

    def test_paging(self):
        path = self._make_file("paging.duckdb")
        handle = self._open(path)
        try:
            values = self._values(handle, 0, [2], start=1, limit=1)
            self.assertSequenceEqual(list(values.channels[0].values.double_array.values), [2.5])
        finally:
            self.service.Close(handle, None)

    def test_default_mode(self):
        path = self._make_file("default_mode.duckdb")
        handle = self._open(path)
        try:
            structure = self._structure(handle)
            group = structure.groups[0]
            self.assertEqual(group.total_number_of_channels, 3)
            self.assertSequenceEqual([ch.id for ch in group.channels], [1, 2, 3])
            self.assertSequenceEqual([ch.name for ch in group.channels], ["1", "2", "3"])
        finally:
            self.service.Close(handle, None)

    def test_mixed_union_member_for_same_channel_id_raises(self):
        path = os.path.join(self.tmp_dir, "mixed_types.duckdb")
        with duckdb.connect(path) as con:
            con.execute(
                "CREATE TABLE mea ("
                "channel_id BIGINT, "
                '"index" BIGINT, '
                "value UNION("
                "DT_DOUBLE DOUBLE, "
                "DT_FLOAT FLOAT, "
                "DT_SHORT SHORT, "
                "DT_LONG INT, "
                "DT_LONGLONG LONG, "
                "DT_STRING VARCHAR"
                "), "
                "flag SHORT"
                ")"
            )
            con.execute("INSERT INTO mea VALUES (42, 0, union_value(DT_DOUBLE := 1.25), 0)")
            con.execute("INSERT INTO mea VALUES (42, 1, union_value(DT_STRING := 'bad'), 0)")

        handle = self._open(path)
        try:
            with self.assertRaises(ValueError):
                self._structure(handle)
        finally:
            self.service.Close(handle, None)
