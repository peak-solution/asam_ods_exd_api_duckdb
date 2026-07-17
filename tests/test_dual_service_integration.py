import unittest
from concurrent import futures
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import grpc
from ods_exd_api_box import ExternalDataReader, FileHandlerRegistry, exd_api, exd_grpc

from external_data_file import ExternalDataFile
from protobuf_generated import mdm_pb2, values_append_pb2_grpc
from protobuf_generated import values_append_pb2 as va
from values_append_service import ValuesAppenderService


class TestDualServiceIntegration(unittest.TestCase):
    def setUp(self):
        FileHandlerRegistry.register(
            file_type_name="DUCKDB",
            file_patterns=["*.duckdb"],
            factory=ExternalDataFile.create,
        )

        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        self._reader = ExternalDataReader()
        exd_grpc.add_ExternalDataReaderServicer_to_server(self._reader, self._server)
        values_append_pb2_grpc.add_ValuesAppenderServicer_to_server(ValuesAppenderService(self._reader), self._server)

        port = self._server.add_insecure_port("127.0.0.1:0")
        if port == 0:
            raise RuntimeError("Failed to bind ephemeral gRPC port")

        self._server.start()
        self._channel = grpc.insecure_channel(f"127.0.0.1:{port}")
        grpc.channel_ready_future(self._channel).result(timeout=5)

        self._reader_stub = exd_grpc.ExternalDataReaderStub(self._channel)
        self._append_stub = values_append_pb2_grpc.ValuesAppenderStub(self._channel)

    def tearDown(self):
        self._channel.close()
        self._reader.stop_auto_close()
        self._server.stop(0)

    def test_append_then_read_via_dual_grpc_services(self):
        with TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "shared.duckdb"
            self._create_normalized_table(str(db_path), "sm1")
            self._seed_initial_value(str(db_path))

            handle = self._reader_stub.Open(exd_api.Identifier(url=db_path.resolve().as_uri(), parameters=""))
            try:
                before = self._reader_stub.GetValues(
                    exd_api.ValuesRequest(handle=handle, group_id=1, channel_ids=[7], start=0, limit=3)
                )
                self.assertSequenceEqual(before.channels[0].values.double_array.values, [10.0])

                write_request = mdm_pb2.WriteRequestList.WriteRequest(channel_group_id="1", channel_id="7")
                write_request.explicit.double_array.values.extend([20.0, 30.0])
                append_request = va.AppendRequest(values=mdm_pb2.WriteRequestList(values=[write_request]))
                append_request.handle.uuid = handle.uuid
                result = self._append_stub.Append(append_request)
                self.assertEqual(dict(result.row_count_by_group), {"1": 2})

                after = self._reader_stub.GetValues(
                    exd_api.ValuesRequest(handle=handle, group_id=1, channel_ids=[7], start=0, limit=3)
                )
                self.assertSequenceEqual(after.channels[0].values.double_array.values, [10.0, 20.0, 30.0])
            finally:
                self._reader_stub.Close(handle)

    def test_init_via_dual_grpc_services(self):
        response = self._append_stub.Init(va.InitRequest(group_ids=[2, 9]))
        self.assertGreater(len(response.duckdb_file_content), 0)

        with TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "init_result.duckdb"
            db_path.write_bytes(response.duckdb_file_content)

            with duckdb.connect(str(db_path), read_only=True) as con:
                tables = con.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name"
                ).fetchall()
                self.assertEqual([row[0] for row in tables], ["sm2", "sm9"])

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

    @staticmethod
    def _seed_initial_value(db_path: str) -> None:
        with duckdb.connect(db_path, read_only=False) as con:
            con.execute(
                "INSERT INTO sm1 VALUES (?, ?, union_value(DT_DOUBLE := ?), ?)",
                [7, 0.0, 10.0, 0],
            )


if __name__ == "__main__":
    unittest.main()
