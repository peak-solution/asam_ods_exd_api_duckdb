"""Dual gRPC server that exposes both EXD reader and ValuesAppender services."""

from __future__ import annotations

import logging
from concurrent import futures

import grpc
from ods_exd_api_box import ExternalDataReader, FileHandlerRegistry, exd_grpc
from ods_exd_api_box.server import (
    _build_server_options,
    _create_health_check_server,
    _create_tls_credentials,
    _get_server_config,
)

from external_data_file import ExternalDataFile
from protobuf_generated.values_append_pb2_grpc import add_ValuesAppenderServicer_to_server
from values_append_service import ValuesAppenderService


def serve_dual() -> None:
    """Start a server with both standard EXD and custom append services."""

    config = _get_server_config()
    if logging.getLogger().isEnabledFor(logging.DEBUG):
        logging.debug("Server configuration: %s", config)

    FileHandlerRegistry.register(
        file_type_name="DUCKDB",
        file_patterns=["*.duckdb"],
        factory=ExternalDataFile.create,
    )

    address = f"{config.bind_address}:{config.port}"
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=config.max_workers),
        options=_build_server_options(config),
    )

    exd_reader = ExternalDataReader(
        auto_close_interval=config.auto_close_interval,
        auto_close_idle=config.auto_close_idle,
    )
    exd_grpc.add_ExternalDataReaderServicer_to_server(exd_reader, server)
    add_ValuesAppenderServicer_to_server(ValuesAppenderService(exd_reader), server)

    if config.use_tls:
        bound_port = server.add_secure_port(address, _create_tls_credentials(config))
        protocol_note = "TLS"
    else:
        bound_port = server.add_insecure_port(address)
        protocol_note = "insecure"

    if bound_port == 0:
        raise RuntimeError(f"Failed to bind gRPC port at {address}")

    health_check_server = _create_health_check_server(config)

    server.start()
    logging.info(
        "Dual gRPC server started (%s), listening on %s with ExternalDataReader and ValuesAppender.",
        protocol_note,
        address,
    )

    try:
        server.wait_for_termination()
    finally:
        exd_reader.stop_auto_close()
        if health_check_server is not None:
            health_check_server.stop(0)
        logging.info("Dual server stopped.")


def main() -> None:
    """Run dual-service server entry point."""

    serve_dual()


if __name__ == "__main__":
    main()
