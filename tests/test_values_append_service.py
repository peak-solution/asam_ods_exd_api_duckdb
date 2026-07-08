import os
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import grpc
from google.protobuf.timestamp_pb2 import Timestamp
from ods_exd_api_box import ExternalDataReader, FileHandlerRegistry, exd_api

from external_data_file import ExternalDataFile
from protobuf_generated import mdm_pb2
from protobuf_generated import values_append_pb2 as va
from tests.mock_servicer_context import MockServicerContext
from values_append_service import ValuesAppenderService


class TestValuesAppenderService(unittest.TestCase):
    def setUp(self):
        FileHandlerRegistry.register(file_type_name="test", factory=ExternalDataFile)
        self.reader = ExternalDataReader()
        self.service = ValuesAppenderService(self.reader)

    def tearDown(self):
        self.reader.stop_auto_close()

    def test_append_rejects_missing_handle(self):
        context = MockServicerContext()

        with self.assertRaises(grpc.RpcError):
            self.service.Append(va.AppendRequest(), context)

        self.assertEqual(context.code(), grpc.StatusCode.INVALID_ARGUMENT)
        self.assertEqual(context.details(), "AppendRequest.handle.uuid must not be empty")

    def test_init_rejects_negative_group_id(self):
        context = MockServicerContext()

        with self.assertRaises(grpc.RpcError):
            self.service.Init(va.InitRequest(group_ids=[1, -1]), context)

        self.assertEqual(context.code(), grpc.StatusCode.INVALID_ARGUMENT)
        self.assertEqual(context.details(), "group_ids[1] must be >= 0")

    def test_init_returns_duckdb_binary_for_group_ids(self):
        context = MockServicerContext()
        response = self.service.Init(va.InitRequest(group_ids=[1, 5, 5]), context)

        self.assertIsNotNone(response)
        self.assertIsNone(context.code())
        self.assertGreater(len(response.duckdb_file_content), 0)

        with TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "init_result.duckdb"
            db_path.write_bytes(response.duckdb_file_content)

            with duckdb.connect(str(db_path), read_only=True) as con:
                tables = con.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name"
                ).fetchall()
                table_names = [row[0] for row in tables]

                self.assertEqual(table_names, ["sm1", "sm5"])

                columns = con.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'main' AND table_name = 'sm1' "
                    "ORDER BY ordinal_position"
                ).fetchall()
                self.assertEqual([row[0] for row in columns], ["channel_id", "index", "value", "flag"])

    def test_append_rejects_empty_values_for_handle(self):
        with TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "shared.duckdb"
            self._create_normalized_table(str(db_path), "sm1")

            handle = self.reader.Open(exd_api.Identifier(url=db_path.resolve().as_uri(), parameters=""), None)
            context = MockServicerContext()

            try:
                with self.assertRaises(grpc.RpcError):
                    request = va.AppendRequest()
                    request.handle.uuid = handle.uuid
                    self.service.Append(request, context)
            finally:
                self.reader.Close(handle, None)

        self.assertEqual(context.code(), grpc.StatusCode.INVALID_ARGUMENT)
        self.assertEqual(context.details(), "WriteRequestList.values must not be empty")

    def test_append_accepts_explicit_double_payload(self):
        with TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "shared.duckdb"
            self._create_normalized_table(str(db_path), "sm1")
            handle = self.reader.Open(exd_api.Identifier(url=db_path.resolve().as_uri(), parameters=""), None)

            context = MockServicerContext()

            write_request = mdm_pb2.WriteRequestList.WriteRequest(
                channel_group_id="1",
                channel_id="7",
            )
            write_request.explicit.double_array.values.extend([1.0, 2.0, 3.0])

            try:
                append_request = va.AppendRequest(values=mdm_pb2.WriteRequestList(values=[write_request]))
                append_request.handle.uuid = handle.uuid
                response = self.service.Append(
                    append_request,
                    context,
                )
            finally:
                self.reader.Close(handle, None)

            self.assertIsNotNone(response)
            self.assertEqual(dict(response.row_count_by_group), {"1": 3})
            self.assertIsNone(context.code())

            with duckdb.connect(str(db_path), read_only=True) as con:
                rows = con.execute(
                    """
                    SELECT CAST("index" AS BIGINT), CAST(union_extract(value, 'DT_DOUBLE') AS DOUBLE), flag
                    FROM sm1
                    WHERE channel_id = 7
                    ORDER BY "index"
                    """
                ).fetchall()
            self.assertEqual(rows, [(0, 1.0, 0), (1, 2.0, 0), (2, 3.0, 0)])

    def test_reader_and_appender_share_same_file(self):
        with TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "shared.duckdb"
            self._create_normalized_table(str(db_path), "sm1")

            with duckdb.connect(str(db_path), read_only=False) as con:
                con.execute(
                    "INSERT INTO sm1 VALUES (?, ?, union_value(DT_DOUBLE := ?), ?)",
                    [7, 0.0, 10.0, 0],
                )

            handle = self.reader.Open(exd_api.Identifier(url=db_path.resolve().as_uri(), parameters=""), None)
            try:
                before = self.reader.GetValues(
                    exd_api.ValuesRequest(handle=handle, group_id=1, channel_ids=[7], start=0, limit=3),
                    None,
                )
                self.assertSequenceEqual(before.channels[0].values.double_array.values, [10.0])

                context = MockServicerContext()
                write_request = mdm_pb2.WriteRequestList.WriteRequest(
                    channel_group_id="1",
                    channel_id="7",
                )
                write_request.explicit.double_array.values.extend([20.0, 30.0])

                append_request = va.AppendRequest(values=mdm_pb2.WriteRequestList(values=[write_request]))
                append_request.handle.uuid = handle.uuid
                response = self.service.Append(
                    append_request,
                    context,
                )
                self.assertIsNone(context.code())
                self.assertEqual(dict(response.row_count_by_group), {"1": 3})

                after = self.reader.GetValues(
                    exd_api.ValuesRequest(handle=handle, group_id=1, channel_ids=[7], start=0, limit=3),
                    None,
                )
                self.assertSequenceEqual(after.channels[0].values.double_array.values, [10.0, 20.0, 30.0])
            finally:
                self.reader.Close(handle, None)

    def test_append_returns_number_of_rows_for_each_touched_table(self):
        with TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "shared.duckdb"
            self._create_normalized_table(str(db_path), "sm1")
            self._create_normalized_table(str(db_path), "sm2")
            handle = self.reader.Open(exd_api.Identifier(url=db_path.resolve().as_uri(), parameters=""), None)

            context = MockServicerContext()

            first = mdm_pb2.WriteRequestList.WriteRequest(channel_group_id="1", channel_id="7")
            first.explicit.double_array.values.extend([1.0])

            second = mdm_pb2.WriteRequestList.WriteRequest(channel_group_id="2", channel_id="8")
            second.explicit.double_array.values.extend([2.0, 3.0])

            request = va.AppendRequest(values=mdm_pb2.WriteRequestList(values=[first, second]))
            request.handle.uuid = handle.uuid

            try:
                response = self.service.Append(request, context)
            finally:
                self.reader.Close(handle, None)

            self.assertIsNone(context.code())
            self.assertEqual(dict(response.row_count_by_group), {"1": 1, "2": 2})

    def test_append_rejects_mismatched_lengths_in_same_group(self):
        with TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "shared.duckdb"
            self._create_normalized_table(str(db_path), "sm1")
            handle = self.reader.Open(exd_api.Identifier(url=db_path.resolve().as_uri(), parameters=""), None)

            first = mdm_pb2.WriteRequestList.WriteRequest(channel_group_id="1", channel_id="7")
            first.explicit.double_array.values.extend([1.0, 2.0])

            second = mdm_pb2.WriteRequestList.WriteRequest(channel_group_id="1", channel_id="8")
            second.explicit.double_array.values.extend([3.0])

            request = va.AppendRequest(values=mdm_pb2.WriteRequestList(values=[first, second]))
            request.handle.uuid = handle.uuid
            context = MockServicerContext()

            try:
                with self.assertRaises(grpc.RpcError):
                    self.service.Append(request, context)
            finally:
                self.reader.Close(handle, None)

            self.assertEqual(context.code(), grpc.StatusCode.INVALID_ARGUMENT)
            self.assertEqual(context.details(), "channel_group_id 1 has mismatched payload lengths: expected 2, got 1")

            with duckdb.connect(str(db_path), read_only=True) as con:
                row_count = con.execute("SELECT COUNT(*) FROM sm1").fetchone()[0]
            self.assertEqual(row_count, 0)

    def test_append_aligns_indices_for_all_channels_in_group(self):
        with TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "shared.duckdb"
            self._create_normalized_table(str(db_path), "sm1")
            handle = self.reader.Open(exd_api.Identifier(url=db_path.resolve().as_uri(), parameters=""), None)

            first = mdm_pb2.WriteRequestList.WriteRequest(channel_group_id="1", channel_id="7")
            first.explicit.double_array.values.extend([1.0, 2.0])

            second = mdm_pb2.WriteRequestList.WriteRequest(channel_group_id="1", channel_id="8")
            second.explicit.double_array.values.extend([10.0, 20.0])

            request = va.AppendRequest(values=mdm_pb2.WriteRequestList(values=[first, second]))
            request.handle.uuid = handle.uuid
            context = MockServicerContext()

            try:
                response = self.service.Append(request, context)
            finally:
                self.reader.Close(handle, None)

            self.assertIsNone(context.code())
            self.assertEqual(dict(response.row_count_by_group), {"1": 2})

            with duckdb.connect(str(db_path), read_only=True) as con:
                rows = con.execute(
                    """
                    SELECT channel_id, CAST("index" AS BIGINT), CAST(union_extract(value, 'DT_DOUBLE') AS DOUBLE)
                    FROM sm1
                    ORDER BY channel_id, "index"
                    """
                ).fetchall()
            self.assertEqual(rows, [(7, 0, 1.0), (7, 1, 2.0), (8, 0, 10.0), (8, 1, 20.0)])

    def test_write_requests_to_dataframe_contains_all_supported_explicit_datatypes(self):
        date_value = Timestamp()
        date_value.FromDatetime(datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc))

        requests: list[mdm_pb2.WriteRequestList.WriteRequest] = []

        req_double = mdm_pb2.WriteRequestList.WriteRequest(channel_group_id="42", channel_id="1")
        req_double.explicit.double_array.values.append(12.3)
        req_double.explicit.flags_full.append(7)
        requests.append(req_double)

        req_float = mdm_pb2.WriteRequestList.WriteRequest(channel_group_id="42", channel_id="2")
        req_float.explicit.float_array.values.append(81.2)
        requests.append(req_float)

        req_short = mdm_pb2.WriteRequestList.WriteRequest(channel_group_id="42", channel_id="3")
        req_short.explicit.short_array.values.append(123)
        requests.append(req_short)

        req_long = mdm_pb2.WriteRequestList.WriteRequest(channel_group_id="42", channel_id="4")
        req_long.explicit.integer_array.values.append(123456)
        requests.append(req_long)

        req_longlong = mdm_pb2.WriteRequestList.WriteRequest(channel_group_id="42", channel_id="5")
        req_longlong.explicit.long_array.values.append(987654321)
        requests.append(req_longlong)

        req_string = mdm_pb2.WriteRequestList.WriteRequest(channel_group_id="42", channel_id="6")
        req_string.explicit.string_array.values.append("hello")
        requests.append(req_string)

        req_boolean = mdm_pb2.WriteRequestList.WriteRequest(channel_group_id="42", channel_id="7")
        req_boolean.explicit.boolean_array.values.append(True)
        req_boolean.explicit.flags.append(True)
        requests.append(req_boolean)

        req_byte = mdm_pb2.WriteRequestList.WriteRequest(channel_group_id="42", channel_id="8")
        req_byte.explicit.byte_array.values = b"\x11"
        requests.append(req_byte)

        req_complex = mdm_pb2.WriteRequestList.WriteRequest(channel_group_id="42", channel_id="9")
        req_complex_value = req_complex.explicit.float_complex_array.values.add()
        req_complex_value.re = 1.5
        req_complex_value.im = -2.5
        requests.append(req_complex)

        req_dcomplex = mdm_pb2.WriteRequestList.WriteRequest(channel_group_id="42", channel_id="10")
        req_dcomplex_value = req_dcomplex.explicit.double_complex_array.values.add()
        req_dcomplex_value.re = 3.25
        req_dcomplex_value.im = -4.75
        requests.append(req_dcomplex)

        req_bytestr = mdm_pb2.WriteRequestList.WriteRequest(channel_group_id="42", channel_id="11")
        req_bytestr.explicit.byte_stream_array.values.append(b"\x01\x02")
        requests.append(req_bytestr)

        req_date = mdm_pb2.WriteRequestList.WriteRequest(channel_group_id="42", channel_id="12")
        req_date.explicit.date_array.values.add().CopyFrom(date_value)
        requests.append(req_date)

        context = MockServicerContext()
        df = self.service._write_requests_to_dataframe(requests, context)

        self.assertEqual(list(df.columns), ["channel_id", "index", "dtype", "value", "flag"])
        self.assertEqual(len(df), 12)
        self.assertIsNone(context.code())

        rows_by_channel_id = {int(row["channel_id"]): row for row in df.to_dict("records")}

        self.assertEqual(rows_by_channel_id[1]["dtype"], "DT_DOUBLE")
        self.assertAlmostEqual(rows_by_channel_id[1]["value"], 12.3, places=7)
        self.assertEqual(rows_by_channel_id[1]["flag"], 7)

        self.assertEqual(rows_by_channel_id[2]["dtype"], "DT_FLOAT")
        self.assertAlmostEqual(rows_by_channel_id[2]["value"], 81.2, places=5)
        self.assertEqual(rows_by_channel_id[2]["flag"], 0)

        self.assertEqual(rows_by_channel_id[3]["dtype"], "DT_SHORT")
        self.assertEqual(rows_by_channel_id[3]["value"], 123)

        self.assertEqual(rows_by_channel_id[4]["dtype"], "DT_LONG")
        self.assertEqual(rows_by_channel_id[4]["value"], 123456)

        self.assertEqual(rows_by_channel_id[5]["dtype"], "DT_LONGLONG")
        self.assertEqual(rows_by_channel_id[5]["value"], 987654321)

        self.assertEqual(rows_by_channel_id[6]["dtype"], "DT_STRING")
        self.assertEqual(rows_by_channel_id[6]["value"], "hello")

        self.assertEqual(rows_by_channel_id[7]["dtype"], "DT_BOOLEAN")
        self.assertTrue(rows_by_channel_id[7]["value"])
        self.assertEqual(rows_by_channel_id[7]["flag"], 1)

        self.assertEqual(rows_by_channel_id[8]["dtype"], "DT_BYTE")
        self.assertEqual(rows_by_channel_id[8]["value"], 17)

        self.assertEqual(rows_by_channel_id[9]["dtype"], "DT_COMPLEX")
        self.assertEqual(rows_by_channel_id[9]["value"], {"re": 1.5, "im": -2.5})

        self.assertEqual(rows_by_channel_id[10]["dtype"], "DT_DCOMPLEX")
        self.assertEqual(rows_by_channel_id[10]["value"], {"re": 3.25, "im": -4.75})

        self.assertEqual(rows_by_channel_id[11]["dtype"], "DT_BYTESTR")
        self.assertEqual(rows_by_channel_id[11]["value"], b"\x01\x02")

        self.assertEqual(rows_by_channel_id[12]["dtype"], "DT_DATE")
        self.assertEqual(rows_by_channel_id[12]["value"], "2026-01-02T03:04:05Z")

        for row in rows_by_channel_id.values():
            self.assertEqual(row["index"], 0.0)

    @unittest.skipUnless(
        os.getenv("RUN_PERFORMANCE_TESTS") == "1",
        "Set RUN_PERFORMANCE_TESTS=1 to run heavy append performance tests.",
    )
    def test_append_performance_100x_10000_single_value_requests(self):
        rounds = int(os.getenv("PERF_APPEND_ROUNDS", "100"))
        columns_per_round = int(os.getenv("PERF_APPEND_COLUMNS", "10000"))
        expected_rows = rounds * columns_per_round

        with TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "perf_shared.duckdb"
            self._create_normalized_table(str(db_path), "sm1")
            handle = self.reader.Open(exd_api.Identifier(url=db_path.resolve().as_uri(), parameters=""), None)

            append_request = va.AppendRequest()
            append_request.handle.uuid = handle.uuid
            for i in range(columns_per_round):
                write_request = mdm_pb2.WriteRequestList.WriteRequest(
                    channel_group_id="1",
                    channel_id="7",
                )
                write_request.explicit.string_array.values.append("teststring")
                append_request.values.values.append(write_request)

            last_response = None
            started_at = time.perf_counter()
            try:
                for round_index in range(1, rounds + 1):
                    context = MockServicerContext()
                    append_started_at = time.perf_counter()
                    last_response = self.service.Append(append_request, context)
                    append_elapsed_seconds = time.perf_counter() - append_started_at
                    self.assertIsNone(context.code())
                    print(f"Append round {round_index}/{rounds} took {append_elapsed_seconds:.3f}s")
            finally:
                self.reader.Close(handle, None)

            elapsed_seconds = time.perf_counter() - started_at
            self.assertIsNotNone(last_response)
            self.assertEqual(dict(last_response.row_count_by_group), {"1": rounds})

            with duckdb.connect(str(db_path), read_only=True) as con:
                total_rows = con.execute("SELECT COUNT(*) FROM sm1").fetchone()[0]
            self.assertEqual(total_rows, expected_rows)

            inserts_per_second = (rounds * columns_per_round) / elapsed_seconds
            print(
                f"Append performance: {rounds} rounds x {columns_per_round} columns, "
                f"{elapsed_seconds:.2f}s, {inserts_per_second:,.0f} inserts/s"
            )

    @staticmethod
    def _create_normalized_table(db_path: str, table_name: str) -> None:
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
        with duckdb.connect(db_path) as con:
            con.execute(f'CREATE TABLE "{table_name}" ({schema})')


if __name__ == "__main__":
    unittest.main()
