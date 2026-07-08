"""One-time script: convert data/simple.mf4 → data/simple.duckdb
and create data/examples/all_datatypes_test.duckdb.

Run once (while asammdf is still available), then commit the .duckdb files.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
from asammdf import MDF

DATA_DIR = Path(__file__).parent

_NP_TO_SQL: dict[np.dtype, str] = {
    np.dtype(np.float64): "DOUBLE",
    np.dtype(np.float32): "FLOAT",
    np.dtype(np.int64): "BIGINT",
    np.dtype(np.uint64): "UBIGINT",
    np.dtype(np.int32): "INTEGER",
    np.dtype(np.uint32): "UINTEGER",
    np.dtype(np.int16): "SMALLINT",
    np.dtype(np.uint16): "USMALLINT",
    np.dtype(np.int8): "TINYINT",
    np.dtype(np.uint8): "UTINYINT",
    np.dtype(np.bool_): "BOOLEAN",
}


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def create_simple_duckdb() -> None:
    src = DATA_DIR / "simple.mf4"
    dst = DATA_DIR / "simple.duckdb"
    dst.unlink(missing_ok=True)

    with MDF(src) as mdf:
        con = duckdb.connect(str(dst))
        try:
            for group_index, group in enumerate(mdf.groups):
                n_channels = len(group.channels)
                signals = mdf.select(
                    [(None, group_index, i) for i in range(n_channels)],
                    raw=False,
                    copy_master=False,
                )

                col_names: list[str] = []
                col_sql_types: list[str] = []
                col_data: list[list] = []

                for signal, channel in zip(signals, group.channels):
                    name = "index" if channel.channel_type == 2 else channel.name
                    sql_type = _NP_TO_SQL.get(signal.samples.dtype, "VARCHAR")
                    col_names.append(name)
                    col_sql_types.append(sql_type)
                    if sql_type == "VARCHAR":
                        col_data.append(list(signal.samples))
                    else:
                        col_data.append(signal.samples.tolist())

                table_name = "measurements"
                col_defs = ", ".join(f"{_q(n)} {t}" for n, t in zip(col_names, col_sql_types))
                con.execute(f"CREATE TABLE {_q(table_name)} ({col_defs})")

                col_list = ", ".join(_q(n) for n in col_names)
                placeholders = ", ".join("?" for _ in col_names)
                rows = list(zip(*col_data))
                con.executemany(
                    f"INSERT INTO {_q(table_name)} ({col_list}) VALUES ({placeholders})",
                    rows,
                )
        finally:
            con.close()
    print(f"Created: {dst}")


def create_all_datatypes_duckdb() -> None:
    dst = DATA_DIR / "examples" / "all_datatypes_test.duckdb"
    dst.unlink(missing_ok=True)

    con = duckdb.connect(str(dst))
    try:
        # group_complex: index + STRUCT float + STRUCT double
        con.execute(
            "CREATE TABLE group_complex ("
            '"index" DOUBLE, '
            "complex_data STRUCT(re FLOAT, im FLOAT), "
            "dcomplex_data STRUCT(re DOUBLE, im DOUBLE)"
            ")"
        )
        con.executemany(
            "INSERT INTO group_complex VALUES (?, ?, ?)",
            [
                (0.0, {"re": 1.0, "im": 2.0}, {"re": 5.0, "im": 6.0}),
                (1.0, {"re": 3.0, "im": 4.0}, {"re": 7.0, "im": 8.0}),
            ],
        )

        # group_int: index + all integer variants
        con.execute(
            "CREATE TABLE group_int ("
            '"index" DOUBLE, '
            "tinyint_data TINYINT, "
            "utinyint_data UTINYINT, "
            "smallint_data SMALLINT, "
            "usmallint_data USMALLINT, "
            "integer_data INTEGER, "
            "uinteger_data UINTEGER, "
            "bigint_data BIGINT, "
            "ubigint_data UBIGINT"
            ")"
        )
        con.executemany(
            "INSERT INTO group_int VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (0.0, -2, 2, -2, 2, -2, 2, -2, 2),
                (1.0, 4, 4, 4, 4, 4, 4, 4, 4),
            ],
        )

        # group_real: index + FLOAT + DOUBLE
        con.execute('CREATE TABLE group_real ("index" DOUBLE, float_data FLOAT, double_data DOUBLE)')
        con.executemany(
            "INSERT INTO group_real VALUES (?, ?, ?)",
            [
                (0.0, 1.1, 2.1),
                (1.0, 1.2, 2.2),
            ],
        )

        # group_string: index + VARCHAR
        con.execute('CREATE TABLE group_string ("index" DOUBLE, string_data VARCHAR)')
        con.executemany(
            "INSERT INTO group_string VALUES (?, ?)",
            [
                (0.0, "abc"),
                (1.0, "def"),
            ],
        )
    finally:
        con.close()
    print(f"Created: {dst}")


if __name__ == "__main__":
    create_simple_duckdb()
    create_all_datatypes_duckdb()
