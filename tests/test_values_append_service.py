import os
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import grpc
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

    @unittest.skipUnless(
        os.getenv("RUN_PERFORMANCE_TESTS") == "1",
        "Set RUN_PERFORMANCE_TESTS=1 to run heavy append performance tests.",
    )
    def test_append_performance_100x_10000_single_value_requests(self):
        rounds = int(os.getenv("PERF_APPEND_ROUNDS", "100"))
        requests_per_round = int(os.getenv("PERF_APPEND_REQUESTS_PER_ROUND", "10000"))
        expected_rows = rounds * requests_per_round

        with TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "perf_shared.duckdb"
            self._create_normalized_table(str(db_path), "sm1")
            handle = self.reader.Open(exd_api.Identifier(url=db_path.resolve().as_uri(), parameters=""), None)

            append_request = va.AppendRequest()
            append_request.handle.uuid = handle.uuid
            for i in range(requests_per_round):
                write_request = mdm_pb2.WriteRequestList.WriteRequest(
                    channel_group_id="1",
                    channel_id="7",
                )
                write_request.explicit.double_array.values.append(1.0)
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
            self.assertEqual(dict(last_response.row_count_by_group), {"1": expected_rows})

            with duckdb.connect(str(db_path), read_only=True) as con:
                total_rows = con.execute("SELECT COUNT(*) FROM sm1").fetchone()[0]
            self.assertEqual(total_rows, expected_rows)

            requests_per_second = (rounds * requests_per_round) / elapsed_seconds
            print(
                f"Append performance: {rounds} rounds x {requests_per_round} requests, "
                f"{elapsed_seconds:.2f}s, {requests_per_second:,.0f} requests/s"
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
