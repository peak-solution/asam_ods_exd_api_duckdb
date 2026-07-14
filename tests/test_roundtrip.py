"""Roundtrip tests: write DuckDB files, read back via ExternalDataFile, verify.

Each test creates a minimal DuckDB file in a TemporaryDirectory, opens it
through the ExdFileInterface and checks that the returned structure and values
match exactly what was written.

DuckDB type to ODS DataTypeEnum mapping tested:
  BOOLEAN      -> DT_BOOLEAN    (boolean_array)
  UTINYINT     -> DT_BYTE       (byte_array)
  TINYINT      -> DT_SHORT      (long_array)
  SMALLINT     -> DT_SHORT      (long_array)
  USMALLINT    -> DT_LONG       (long_array)
  INTEGER      -> DT_LONG       (long_array)
  UINTEGER     -> DT_LONGLONG   (longlong_array)
  BIGINT       -> DT_LONGLONG   (longlong_array)
  UBIGINT      -> DT_DOUBLE     (double_array)   <- special case
  FLOAT        -> DT_FLOAT      (float_array)
  DOUBLE       -> DT_DOUBLE     (double_array)
  VARCHAR      -> DT_STRING     (string_array)
  BLOB         -> DT_BYTESTR    (bytestr_array)
  STRUCT(re FLOAT, im FLOAT)   -> DT_COMPLEX   (float_array, interleaved)
  STRUCT(re DOUBLE, im DOUBLE) -> DT_DCOMPLEX  (double_array, interleaved)
"""

import os
import tempfile
import unittest
from typing import cast

import duckdb
from ods_exd_api_box import ExternalDataReader, FileHandlerRegistry, exd_api, ods

from external_data_file import ExternalDataFile

# pylint: disable=E1101


def _uri(path: str) -> str:
    from pathlib import Path

    return Path(path).resolve().as_uri()


class TestRoundtrip(unittest.TestCase):
    """Write DuckDB files, read back through ExternalDataFile, assert correctness."""

    def setUp(self):
        FileHandlerRegistry.register(file_type_name="test", factory=ExternalDataFile)
        self.service = ExternalDataReader()
        self.tmp = tempfile.TemporaryDirectory()
        self.tmp_dir = self.tmp.name

    def tearDown(self):
        self.tmp.cleanup()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _make_file(self, name: str, channels: dict, *, table: str = "measurements") -> str:
        """Create a DuckDB file with one table.

        channels: dict of col_name -> (sql_type, list_of_values)
        """
        path = os.path.join(self.tmp_dir, name)

        def _member_from_sql_type(sql_type: str) -> str:
            t = sql_type.upper()
            if t in ("FLOAT", "REAL"):
                return "DT_FLOAT"
            if t == "BOOLEAN":
                return "DT_BOOLEAN"
            if t == "UTINYINT":
                return "DT_BYTE"
            if t in ("TINYINT", "SMALLINT"):
                return "DT_SHORT"
            if t in ("USMALLINT", "INTEGER"):
                return "DT_LONG"
            if t in ("UINTEGER", "BIGINT"):
                return "DT_LONGLONG"
            if t.startswith("STRUCT("):
                if "RE FLOAT" in t or "RE REAL" in t:
                    return "DT_COMPLEX"
                return "DT_DCOMPLEX"
            if t in ("VARCHAR", "TEXT", "STRING"):
                return "DT_STRING"
            if t == "BLOB":
                return "DT_BYTESTR"
            if t.startswith("TIMESTAMP") or t in ("DATE", "TIME"):
                return "DT_DATE"
            return "DT_DOUBLE"

        with duckdb.connect(path) as con:
            con.execute(
                f'CREATE TABLE "{table}" ('
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

            index_values = list(channels.get("index", ("DOUBLE", []))[1])
            if index_values:
                con.executemany(
                    f'INSERT INTO "{table}" VALUES (?, ?, union_value(DT_DOUBLE := ?), ?)',
                    [(0, idx, float(idx), 0) for idx in index_values],
                )

            channel_id = 1
            for col_name, (sql_type, values) in channels.items():
                if col_name == "index":
                    continue
                member = _member_from_sql_type(sql_type)
                con.executemany(
                    f'INSERT INTO "{table}" VALUES (?, ?, union_value({member} := ?), ?)',
                    [(channel_id, index_values[i], values[i], 0) for i in range(len(index_values))],
                )
                channel_id += 1
        return path

    def _open(self, path: str):
        return self.service.Open(exd_api.Identifier(url=_uri(path), parameters=""), None)

    def _structure(self, handle) -> exd_api.StructureResult:
        return cast(
            exd_api.StructureResult,
            self.service.GetStructure(exd_api.StructureRequest(handle=handle), None),
        )

    def _values(self, handle, group_id, channel_ids, start=0, limit=9999, context=None) -> exd_api.ValuesResult:
        return cast(
            exd_api.ValuesResult,
            self.service.GetValues(
                exd_api.ValuesRequest(
                    handle=handle, group_id=group_id, channel_ids=channel_ids, start=start, limit=limit
                ),
                context,
            ),
        )

    def _assert_floats(self, actual, expected, places=5):
        self.assertEqual(len(actual), len(expected))
        for i, (a, e) in enumerate(zip(actual, expected)):
            self.assertAlmostEqual(float(a), float(e), places=places, msg=f"index {i}: {a} != {e}")

    # ==================================================================
    # Floating-point types
    # ==================================================================

    def test_float64_roundtrip(self):
        vals = [1.1, 2.2, -3.3]
        path = self._make_file(
            "f64.duckdb",
            {
                "index": ("DOUBLE", [0.0, 1.0, 2.0]),
                "ch_f64": ("DOUBLE", vals),
            },
        )
        handle = self._open(path)
        try:
            ch = self._structure(handle).groups[0].channels[1]
            self.assertEqual(ch.data_type, ods.DataTypeEnum.DT_DOUBLE)
            self.assertEqual(ch.unit_string, "")
            result = self._values(handle, 0, [1])
            self._assert_floats(result.channels[0].values.double_array.values, vals)
        finally:
            self.service.Close(handle, None)

    def test_float32_roundtrip(self):
        vals = [1.5, -2.25, 0.0]
        path = self._make_file(
            "f32.duckdb",
            {
                "index": ("DOUBLE", [0.0, 1.0, 2.0]),
                "ch_f32": ("FLOAT", vals),
            },
        )
        handle = self._open(path)
        try:
            ch = self._structure(handle).groups[0].channels[1]
            self.assertEqual(ch.data_type, ods.DataTypeEnum.DT_FLOAT)
            result = self._values(handle, 0, [1])
            self._assert_floats(result.channels[0].values.float_array.values, vals)
        finally:
            self.service.Close(handle, None)

    # ==================================================================
    # Integer types
    # ==================================================================

    def test_tinyint_roundtrip(self):
        """TINYINT (signed 8-bit) -> DT_SHORT / long_array."""
        vals = [-128, 0, 127]
        path = self._make_file("i8.duckdb", {"index": ("DOUBLE", [0.0, 1.0, 2.0]), "ch_i8": ("TINYINT", vals)})
        handle = self._open(path)
        try:
            ch = self._structure(handle).groups[0].channels[1]
            self.assertEqual(ch.data_type, ods.DataTypeEnum.DT_SHORT)
            result = self._values(handle, 0, [1])
            self.assertSequenceEqual(list(result.channels[0].values.long_array.values), vals)
        finally:
            self.service.Close(handle, None)

    def test_utinyint_roundtrip(self):
        """UTINYINT (unsigned 8-bit) -> DT_BYTE / byte_array."""
        vals = [0, 128, 255]
        path = self._make_file("u8.duckdb", {"index": ("DOUBLE", [0.0, 1.0, 2.0]), "ch_u8": ("UTINYINT", vals)})
        handle = self._open(path)
        try:
            ch = self._structure(handle).groups[0].channels[1]
            self.assertEqual(ch.data_type, ods.DataTypeEnum.DT_BYTE)
            result = self._values(handle, 0, [1])
            self.assertSequenceEqual(list(result.channels[0].values.byte_array.values), vals)
        finally:
            self.service.Close(handle, None)

    def test_smallint_roundtrip(self):
        """SMALLINT (signed 16-bit) -> DT_SHORT / long_array."""
        vals = [-1000, 0, 1000]
        path = self._make_file("i16.duckdb", {"index": ("DOUBLE", [0.0, 1.0, 2.0]), "ch_i16": ("SMALLINT", vals)})
        handle = self._open(path)
        try:
            ch = self._structure(handle).groups[0].channels[1]
            self.assertEqual(ch.data_type, ods.DataTypeEnum.DT_SHORT)
            result = self._values(handle, 0, [1])
            self.assertSequenceEqual(list(result.channels[0].values.long_array.values), vals)
        finally:
            self.service.Close(handle, None)

    def test_usmallint_roundtrip(self):
        """USMALLINT (unsigned 16-bit) -> DT_LONG / long_array."""
        vals = [0, 1000, 65535]
        path = self._make_file("u16.duckdb", {"index": ("DOUBLE", [0.0, 1.0, 2.0]), "ch_u16": ("USMALLINT", vals)})
        handle = self._open(path)
        try:
            ch = self._structure(handle).groups[0].channels[1]
            self.assertEqual(ch.data_type, ods.DataTypeEnum.DT_LONG)
            result = self._values(handle, 0, [1])
            self.assertSequenceEqual(list(result.channels[0].values.long_array.values), vals)
        finally:
            self.service.Close(handle, None)

    def test_integer_roundtrip(self):
        """INTEGER (signed 32-bit) -> DT_LONG / long_array."""
        vals = [-100000, 0, 100000]
        path = self._make_file("i32.duckdb", {"index": ("DOUBLE", [0.0, 1.0, 2.0]), "ch_i32": ("INTEGER", vals)})
        handle = self._open(path)
        try:
            ch = self._structure(handle).groups[0].channels[1]
            self.assertEqual(ch.data_type, ods.DataTypeEnum.DT_LONG)
            result = self._values(handle, 0, [1])
            self.assertSequenceEqual(list(result.channels[0].values.long_array.values), vals)
        finally:
            self.service.Close(handle, None)

    def test_uinteger_roundtrip(self):
        """UINTEGER (unsigned 32-bit) -> DT_LONGLONG / longlong_array."""
        vals = [0, 50000, 4294967295]
        path = self._make_file("u32.duckdb", {"index": ("DOUBLE", [0.0, 1.0, 2.0]), "ch_u32": ("UINTEGER", vals)})
        handle = self._open(path)
        try:
            ch = self._structure(handle).groups[0].channels[1]
            self.assertEqual(ch.data_type, ods.DataTypeEnum.DT_LONGLONG)
            result = self._values(handle, 0, [1])
            self.assertSequenceEqual(list(result.channels[0].values.longlong_array.values), vals)
        finally:
            self.service.Close(handle, None)

    def test_bigint_roundtrip(self):
        """BIGINT (signed 64-bit) -> DT_LONGLONG / longlong_array."""
        vals = [-(2**40), 0, 2**40]
        path = self._make_file("i64.duckdb", {"index": ("DOUBLE", [0.0, 1.0, 2.0]), "ch_i64": ("BIGINT", vals)})
        handle = self._open(path)
        try:
            ch = self._structure(handle).groups[0].channels[1]
            self.assertEqual(ch.data_type, ods.DataTypeEnum.DT_LONGLONG)
            result = self._values(handle, 0, [1])
            self.assertSequenceEqual(list(result.channels[0].values.longlong_array.values), vals)
        finally:
            self.service.Close(handle, None)

    def test_ubigint_roundtrip(self):
        """UBIGINT (unsigned 64-bit) -> DT_DOUBLE / double_array (special case)."""
        vals = [0, 2**32, 2**52]
        path = self._make_file("u64.duckdb", {"index": ("DOUBLE", [0.0, 1.0, 2.0]), "ch_u64": ("UBIGINT", vals)})
        handle = self._open(path)
        try:
            ch = self._structure(handle).groups[0].channels[1]
            self.assertEqual(ch.data_type, ods.DataTypeEnum.DT_DOUBLE)
            result = self._values(handle, 0, [1])
            self._assert_floats(result.channels[0].values.double_array.values, [float(v) for v in vals])
        finally:
            self.service.Close(handle, None)

    def test_boolean_roundtrip(self):
        """BOOLEAN -> DT_BOOLEAN / boolean_array."""
        vals = [True, False, True]
        path = self._make_file("bool.duckdb", {"index": ("DOUBLE", [0.0, 1.0, 2.0]), "ch_bool": ("BOOLEAN", vals)})
        handle = self._open(path)
        try:
            ch = self._structure(handle).groups[0].channels[1]
            self.assertEqual(ch.data_type, ods.DataTypeEnum.DT_BOOLEAN)
            result = self._values(handle, 0, [1])
            self.assertSequenceEqual(list(result.channels[0].values.boolean_array.values), vals)
        finally:
            self.service.Close(handle, None)

    # ==================================================================
    # String and BLOB
    # ==================================================================

    def test_varchar_roundtrip(self):
        vals = ["hello", "world", "\u03b1\u03b2\u03b3"]
        path = self._make_file("str.duckdb", {"index": ("DOUBLE", [0.0, 1.0, 2.0]), "ch_str": ("VARCHAR", vals)})
        handle = self._open(path)
        try:
            ch = self._structure(handle).groups[0].channels[1]
            self.assertEqual(ch.data_type, ods.DataTypeEnum.DT_STRING)
            result = self._values(handle, 0, [1])
            self.assertSequenceEqual(list(result.channels[0].values.string_array.values), vals)
        finally:
            self.service.Close(handle, None)

    def test_blob_roundtrip(self):
        """BLOB -> DT_BYTESTR / bytestr_array."""
        blobs = [b"\x01\x02", b"\x03\x04"]
        path = self._make_file("blob.duckdb", {"index": ("DOUBLE", [0.0, 1.0]), "ch_blob": ("BLOB", blobs)})
        handle = self._open(path)
        try:
            ch = self._structure(handle).groups[0].channels[1]
            self.assertEqual(ch.data_type, ods.DataTypeEnum.DT_BYTESTR)
            result = self._values(handle, 0, [1])
            self.assertEqual(list(result.channels[0].values.bytestr_array.values), blobs)
        finally:
            self.service.Close(handle, None)

    # ==================================================================
    # Complex types (STRUCT)
    # ==================================================================

    def test_complex_float_roundtrip(self):
        """STRUCT(re FLOAT, im FLOAT) -> DT_COMPLEX; values interleaved re0,im0,..."""
        structs = [{"re": 1.0, "im": 2.0}, {"re": 3.0, "im": 4.0}]
        path = self._make_file(
            "c32.duckdb",
            {"index": ("DOUBLE", [0.0, 1.0]), "ch_c32": ("STRUCT(re FLOAT, im FLOAT)", structs)},
        )
        handle = self._open(path)
        try:
            ch = self._structure(handle).groups[0].channels[1]
            self.assertEqual(ch.data_type, ods.DataTypeEnum.DT_COMPLEX)
            result = self._values(handle, 0, [1])
            self._assert_floats(result.channels[0].values.float_array.values, [1.0, 2.0, 3.0, 4.0])
        finally:
            self.service.Close(handle, None)

    def test_complex_double_roundtrip(self):
        """STRUCT(re DOUBLE, im DOUBLE) -> DT_DCOMPLEX; values interleaved re0,im0,..."""
        structs = [{"re": 5.0, "im": 6.0}, {"re": 7.0, "im": 8.0}]
        path = self._make_file(
            "c64.duckdb",
            {"index": ("DOUBLE", [0.0, 1.0]), "ch_c64": ("STRUCT(re DOUBLE, im DOUBLE)", structs)},
        )
        handle = self._open(path)
        try:
            ch = self._structure(handle).groups[0].channels[1]
            self.assertEqual(ch.data_type, ods.DataTypeEnum.DT_DCOMPLEX)
            result = self._values(handle, 0, [1])
            self._assert_floats(result.channels[0].values.double_array.values, [5.0, 6.0, 7.0, 8.0])
        finally:
            self.service.Close(handle, None)

    # ==================================================================
    # Multiple groups / channels
    # ==================================================================

    def test_multiple_groups_roundtrip(self):
        """Multiple tables are addressed by group_id (sorted alphabetically)."""
        path = os.path.join(self.tmp_dir, "multigrp.duckdb")
        with duckdb.connect(path) as con:
            for table_name in ("grp0", "grp1"):
                con.execute(
                    f'CREATE TABLE "{table_name}" ('
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

            grp0_idx = [0.0, 1.0, 2.0]
            con.executemany(
                "INSERT INTO grp0 VALUES (?, ?, union_value(DT_DOUBLE := ?), ?)",
                [(0, i, i, 0) for i in grp0_idx],
            )
            con.executemany(
                "INSERT INTO grp0 VALUES (?, ?, union_value(DT_DOUBLE := ?), ?)",
                [(1, i, v, 0) for i, v in zip(grp0_idx, [1.0, 2.0, 3.0])],
            )

            grp1_idx = [0.0, 1.0, 2.0]
            con.executemany(
                "INSERT INTO grp1 VALUES (?, ?, union_value(DT_DOUBLE := ?), ?)",
                [(0, i, i, 0) for i in grp1_idx],
            )
            con.executemany(
                "INSERT INTO grp1 VALUES (?, ?, union_value(DT_LONG := ?), ?)",
                [(1, i, v, 0) for i, v in zip(grp1_idx, [10, 20, 30])],
            )
        handle = self._open(path)
        try:
            structure = self._structure(handle)
            self.assertEqual(len(structure.groups), 2)
            self.assertEqual(structure.groups[0].id, 0)
            self.assertEqual(structure.groups[1].id, 1)
            r0 = self._values(handle, 0, [1])
            self._assert_floats(r0.channels[0].values.double_array.values, [1.0, 2.0, 3.0])
            r1 = self._values(handle, 1, [1])
            self.assertSequenceEqual(list(r1.channels[0].values.long_array.values), [10, 20, 30])
        finally:
            self.service.Close(handle, None)

    def test_multiple_channels_roundtrip(self):
        """All channels in a group are individually addressable."""
        path = self._make_file(
            "multichan.duckdb",
            {
                "index": ("DOUBLE", [0.0, 1.0, 2.0]),
                "ch_a": ("DOUBLE", [1.0, 2.0, 3.0]),
                "ch_b": ("INTEGER", [4, 5, 6]),
                "ch_c": ("VARCHAR", ["x", "y", "z"]),
            },
        )
        handle = self._open(path)
        try:
            structure = self._structure(handle)
            self.assertEqual(len(structure.groups[0].channels), 4)
            result = self._values(handle, 0, [1, 2, 3])
            self.assertEqual(len(result.channels), 3)
            self._assert_floats(result.channels[0].values.double_array.values, [1.0, 2.0, 3.0])
            self.assertSequenceEqual(list(result.channels[1].values.long_array.values), [4, 5, 6])
            self.assertSequenceEqual(list(result.channels[2].values.string_array.values), ["x", "y", "z"])
        finally:
            self.service.Close(handle, None)

    # ==================================================================
    # Paging: start / limit
    # ==================================================================

    def test_paging_middle_slice(self):
        n = 50
        vals = list(map(float, range(n)))
        path = self._make_file(
            "paging.duckdb",
            {"index": ("DOUBLE", list(map(float, range(n)))), "ramp": ("DOUBLE", vals)},
        )
        handle = self._open(path)
        try:
            result = self._values(handle, 0, [1], start=10, limit=5)
            self._assert_floats(result.channels[0].values.double_array.values, vals[10:15])
        finally:
            self.service.Close(handle, None)

    def test_paging_limit_clamps_at_end(self):
        path = self._make_file(
            "clamp.duckdb", {"index": ("DOUBLE", [0.0, 1.0, 2.0]), "ch": ("DOUBLE", [1.0, 2.0, 3.0])}
        )
        handle = self._open(path)
        try:
            result = self._values(handle, 0, [1], start=0, limit=9999)
            self.assertEqual(len(result.channels[0].values.double_array.values), 3)
        finally:
            self.service.Close(handle, None)

    def test_paging_first_row_only(self):
        path = self._make_file(
            "first.duckdb", {"index": ("DOUBLE", [0.0, 1.0, 2.0]), "ch": ("DOUBLE", [10.0, 20.0, 30.0])}
        )
        handle = self._open(path)
        try:
            result = self._values(handle, 0, [1], start=0, limit=1)
            self._assert_floats(result.channels[0].values.double_array.values, [10.0])
        finally:
            self.service.Close(handle, None)

    def test_paging_last_row(self):
        path = self._make_file(
            "last.duckdb", {"index": ("DOUBLE", [0.0, 1.0, 2.0]), "ch": ("DOUBLE", [10.0, 20.0, 30.0])}
        )
        handle = self._open(path)
        try:
            result = self._values(handle, 0, [1], start=2, limit=1)
            self._assert_floats(result.channels[0].values.double_array.values, [30.0])
        finally:
            self.service.Close(handle, None)

    def test_paging_multiple_channels_same_slice(self):
        n = 20
        path = self._make_file(
            "slice_multi.duckdb",
            {
                "index": ("DOUBLE", list(map(float, range(n)))),
                "a": ("DOUBLE", [float(i) for i in range(n)]),
                "b": ("INTEGER", [i * 2 for i in range(n)]),
            },
        )
        handle = self._open(path)
        try:
            result = self._values(handle, 0, [1, 2], start=5, limit=4)
            self._assert_floats(result.channels[0].values.double_array.values, [5.0, 6.0, 7.0, 8.0])
            self.assertSequenceEqual(list(result.channels[1].values.long_array.values), [10, 12, 14, 16])
        finally:
            self.service.Close(handle, None)

    def test_non_continuous_index_fills_invalid_values(self):
        """Missing integer index positions are padded with invalid placeholders and flag=0."""
        path = os.path.join(self.tmp_dir, "gaps.duckdb")
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
                [(0, 0.0, 0.0, 1), (0, 2.0, 2.0, 1), (0, 4.0, 4.0, 1)],
            )
            con.executemany(
                "INSERT INTO measurements VALUES (?, ?, union_value(DT_DOUBLE := ?), ?)",
                [(1, 0.0, 10.0, 1), (1, 2.0, 30.0, 1), (1, 4.0, 50.0, 1)],
            )

        handle = self._open(path)
        try:
            structure = self._structure(handle)
            self.assertEqual(structure.groups[0].number_of_rows, 5)

            result = self._values(handle, 0, [0, 1], start=0, limit=10)
            self._assert_floats(result.channels[0].values.double_array.values, [0.0, 0.0, 2.0, 0.0, 4.0])
            self._assert_floats(result.channels[1].values.double_array.values, [10.0, 0.0, 30.0, 0.0, 50.0])
            self.assertSequenceEqual(list(result.channels[0].flags.values), [1, 0, 1, 0, 1])
            self.assertSequenceEqual(list(result.channels[1].flags.values), [1, 0, 1, 0, 1])
        finally:
            self.service.Close(handle, None)

    # ==================================================================
    # Edge cases
    # ==================================================================

    def test_single_row_roundtrip(self):
        path = self._make_file("single.duckdb", {"index": ("DOUBLE", [0.0]), "ch": ("DOUBLE", [42.0])})
        handle = self._open(path)
        try:
            self.assertEqual(self._structure(handle).groups[0].number_of_rows, 1)
            result = self._values(handle, 0, [1])
            self._assert_floats(result.channels[0].values.double_array.values, [42.0])
        finally:
            self.service.Close(handle, None)

    def test_empty_table_structure(self):
        """A table with zero rows must be reported with number_of_rows == 0."""
        path = os.path.join(self.tmp_dir, "empty.duckdb")
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
        handle = self._open(path)
        try:
            structure = self._structure(handle)
            self.assertEqual(structure.groups[0].number_of_rows, 0)
        finally:
            self.service.Close(handle, None)

    # ==================================================================
    # Channel / group metadata
    # ==================================================================

    def test_channel_has_no_unit_or_description(self):
        """DuckDB has no metadata storage; unit_string is empty and description absent."""
        path = self._make_file("no_meta.duckdb", {"index": ("DOUBLE", [0.0]), "speed": ("DOUBLE", [1.0])})
        handle = self._open(path)
        try:
            ch = self._structure(handle).groups[0].channels[1]
            self.assertEqual(ch.unit_string, "")
            self.assertNotIn("description", ch.attributes.variables)
        finally:
            self.service.Close(handle, None)

    def test_index_column_is_independent(self):
        """Column named 'index' must carry independent=1; other columns must not."""
        path = self._make_file("indep.duckdb", {"index": ("DOUBLE", [0.0, 1.0]), "ch": ("DOUBLE", [1.0, 2.0])})
        handle = self._open(path)
        try:
            grp = self._structure(handle).groups[0]
            index_ch = grp.channels[0]
            self.assertIn("independent", index_ch.attributes.variables)
            self.assertEqual(index_ch.attributes.variables["independent"].long_array.values[0], 1)
            data_ch = grp.channels[1]
            self.assertNotIn("independent", data_ch.attributes.variables)
        finally:
            self.service.Close(handle, None)

    def test_index_channel_values(self):
        """Reading channel 0 (index) returns the stored index values."""
        ts = [0.0, 0.5, 1.0]
        path = self._make_file("time_vals.duckdb", {"index": ("DOUBLE", ts), "ch": ("DOUBLE", [10.0, 20.0, 30.0])})
        handle = self._open(path)
        try:
            result = self._values(handle, 0, [0])
            self.assertEqual(result.channels[0].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
            self._assert_floats(result.channels[0].values.double_array.values, ts)
        finally:
            self.service.Close(handle, None)

    def test_group_name_equals_table_name(self):
        """Group name must equal the DuckDB table name."""
        path = os.path.join(self.tmp_dir, "grp_meta.duckdb")
        with duckdb.connect(path) as con:
            con.execute(
                "CREATE TABLE my_group ("
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
            con.execute("INSERT INTO my_group VALUES (0, 0.0, union_value(DT_DOUBLE := 0.0), 0)")
            con.execute("INSERT INTO my_group VALUES (1, 0.0, union_value(DT_DOUBLE := 1.0), 0)")
        handle = self._open(path)
        try:
            grp = self._structure(handle).groups[0]
            self.assertEqual(grp.name, "my_group")
        finally:
            self.service.Close(handle, None)

    def test_start_time_is_present(self):
        """start_time attribute must be present and non-empty (uses datetime.now())."""
        path = self._make_file("meas_begin.duckdb", {"index": ("DOUBLE", [0.0])})
        handle = self._open(path)
        try:
            structure = self._structure(handle)
            start_time = structure.attributes.variables.get("start_time")
            self.assertIsNotNone(start_time)
            self.assertGreater(len(start_time.string_array.values[0]), 0)
        finally:
            self.service.Close(handle, None)

    def test_total_number_of_channels(self):
        ts = [0.0, 1.0]
        path = self._make_file(
            "total_ch.duckdb",
            {"index": ("DOUBLE", ts), **{f"ch{i}": ("DOUBLE", [float(i)] * 2) for i in range(5)}},
        )
        handle = self._open(path)
        try:
            grp = self._structure(handle).groups[0]
            self.assertEqual(grp.total_number_of_channels, 6)
            self.assertEqual(len(grp.channels), 6)
        finally:
            self.service.Close(handle, None)

    def test_structure_identifier_url_is_set(self):
        path = self._make_file("url.duckdb", {"index": ("DOUBLE", [0.0])})
        handle = self._open(path)
        try:
            self.assertNotEqual(self._structure(handle).identifier.url, "")
        finally:
            self.service.Close(handle, None)

    def test_values_result_carries_group_id(self):
        """ValuesResult.id must equal the requested group_id."""
        path = os.path.join(self.tmp_dir, "grp_id.duckdb")
        with duckdb.connect(path) as con:
            for i in range(3):
                con.execute(
                    f"CREATE TABLE table{i} ("
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
                    f"INSERT INTO table{i} VALUES (?, ?, union_value(DT_DOUBLE := ?), ?)",
                    [(0, 0.0, 0.0, 0), (0, 1.0, 1.0, 0)],
                )
                con.executemany(
                    f"INSERT INTO table{i} VALUES (?, ?, union_value(DT_DOUBLE := ?), ?)",
                    [(1, 0.0, float(i), 0), (1, 1.0, float(i), 0)],
                )
        handle = self._open(path)
        try:
            for gid in range(3):
                result = self._values(handle, gid, [1])
                self.assertEqual(result.id, gid)
        finally:
            self.service.Close(handle, None)

    # ==================================================================
    # Error cases
    # ==================================================================

    def test_invalid_group_id_raises(self):
        path = self._make_file("err_grp.duckdb", {"index": ("DOUBLE", [0.0]), "ch": ("DOUBLE", [1.0])})
        handle = self._open(path)
        try:
            with self.assertRaises(ValueError):
                self._values(handle, 999, [0])
        finally:
            self.service.Close(handle, None)

    def test_invalid_channel_id_raises(self):
        path = self._make_file("err_ch.duckdb", {"index": ("DOUBLE", [0.0]), "ch": ("DOUBLE", [1.0])})
        handle = self._open(path)
        try:
            with self.assertRaises(ValueError):
                self._values(handle, 0, [999])
        finally:
            self.service.Close(handle, None)

    def test_start_beyond_row_count_raises(self):
        path = self._make_file("err_start.duckdb", {"index": ("DOUBLE", [0.0, 1.0]), "ch": ("DOUBLE", [1.0, 2.0])})
        handle = self._open(path)
        try:
            with self.assertRaises(ValueError):
                self._values(handle, 0, [1], start=999)
        finally:
            self.service.Close(handle, None)
