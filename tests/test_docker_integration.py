import logging
import pathlib
import socket
import subprocess
import tempfile
import time
import unittest

import duckdb
import grpc
from google.protobuf.json_format import MessageToJson
from ods_exd_api_box import exd_api, exd_grpc, ods


class TestDockerContainer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._tmp_dir_obj = tempfile.TemporaryDirectory()
        cls._tmp_dir = pathlib.Path(cls._tmp_dir_obj.name)
        cls._make_simple_file(str(cls._tmp_dir / "simple.duckdb"))

        # Docker-Image bauen
        subprocess.run(["docker", "build", "-t", "asam-ods-exd-api-duckdb", "."], check=True)

        # Remove any stale container with the same name
        subprocess.run(["docker", "rm", "-f", "test_container"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        cls._db_path_in_container = "/tmp/simple.duckdb"
        cp = subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--rm",
                "--name",
                "test_container",
                "-p",
                "50051:50051",
                "asam-ods-exd-api-duckdb",
            ],
            stdout=subprocess.PIPE,
            check=True,
        )
        cls.container_id = cp.stdout.decode().strip()

        # Avoid host bind-mount UID/permission mismatch in CI by copying directly
        # into the running container and fixing ownership/permissions there.
        subprocess.run(
            ["docker", "cp", str(cls._tmp_dir / "simple.duckdb"), f"test_container:{cls._db_path_in_container}"],
            check=True,
        )
        subprocess.run(
            [
                "docker",
                "exec",
                "-u",
                "0",
                "test_container",
                "sh",
                "-lc",
                f"chown appuser:appuser {cls._db_path_in_container} && chmod 0644 {cls._db_path_in_container}",
            ],
            check=True,
        )
        subprocess.run(
            ["docker", "exec", "test_container", "test", "-r", cls._db_path_in_container],
            check=True,
        )

        cls.__wait_for_port_ready()

    @classmethod
    def tearDownClass(cls):
        # Container stoppen
        subprocess.run(["docker", "stop", "test_container"], check=True)
        cls._tmp_dir_obj.cleanup()

    @staticmethod
    def _make_simple_file(path: str) -> None:
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

    @classmethod
    def __wait_for_port_ready(cls, host="localhost", port=50051, timeout=30):
        start_time = time.time()
        while True:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                return
            if time.time() - start_time > timeout:
                raise TimeoutError("Port did not become ready in time.")
            time.sleep(0.5)

    def test_container_health(self):
        channel = grpc.insecure_channel("localhost:50051")
        stub = exd_grpc.ExternalDataReaderStub(channel)
        self.assertIsNotNone(stub)
        grpc.channel_ready_future(channel).result(timeout=5)

    def test_structure(self):
        with grpc.insecure_channel("localhost:50051") as channel:
            service = exd_grpc.ExternalDataReaderStub(channel)

            handle = service.Open(exd_api.Identifier(url=self._db_path_in_container, parameters=""), None)
            try:
                structure = service.GetStructure(exd_api.StructureRequest(handle=handle), None)
                logging.info(MessageToJson(structure))

                self.assertEqual(structure.name, "simple.duckdb")
                self.assertEqual(len(structure.groups), 1)
                self.assertEqual(structure.groups[0].number_of_rows, 4)
                self.assertEqual(len(structure.groups[0].channels), 4)
                self.assertEqual(structure.groups[0].id, 0)
                logging.info(MessageToJson(structure))
                self.assertEqual(structure.groups[0].channels[0].id, 0)
            finally:
                service.Close(handle, None)

    def test_get_values(self):
        with grpc.insecure_channel("localhost:50051") as channel:
            service = exd_grpc.ExternalDataReaderStub(channel)

            handle = service.Open(exd_api.Identifier(url=self._db_path_in_container, parameters=""), None)

            try:
                values = service.GetValues(
                    exd_api.ValuesRequest(handle=handle, group_id=0, channel_ids=[0, 1, 2, 3], start=0, limit=4),
                    None,  #
                )
                self.assertEqual(values.id, 0)
                self.assertEqual(len(values.channels), 4)
                self.assertEqual(values.channels[0].id, 0)
                self.assertEqual(values.channels[1].id, 1)
                logging.info(MessageToJson(values))

                self.assertEqual(values.channels[0].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertSequenceEqual(values.channels[0].values.double_array.values, [0.0, 1.0, 2.0, 3.0])
                self.assertEqual(values.channels[1].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertSequenceEqual(values.channels[1].values.double_array.values, [10.0, 20.0, 30.0, 40.0])
                self.assertEqual(values.channels[2].values.data_type, ods.DataTypeEnum.DT_LONGLONG)
                self.assertSequenceEqual(values.channels[2].values.longlong_array.values, [1000, 2000, 3000, 4000])
            finally:
                service.Close(handle, None)
