import logging
import traceback
import uuid
from typing import Any, Iterable, Mapping

from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import (
    AirbyteConnectionStatus,
    AirbyteMessage,
    ConfiguredAirbyteCatalog,
    DestinationSyncMode,
    Status,
    Type,
)
from destination_sftp_csv.client import SftpClient


def _flatten_config(config: Mapping[str, Any]) -> dict:
    """
    Flattens nested sections from the Airbyte UI config schema,
    such as `connection` and `csv_options`, into top-level properties
    expected by SftpClient.
    """
    flat = dict(config)

    # Flatten connection.* → host, port, username, password, …
    if "connection" in flat:
        flat.update(flat["connection"])
        del flat["connection"]

    # Flatten csv_options.* → extraction_mode, separator, encoding, etc.
    if "csv_options" in flat:
        flat.update(flat["csv_options"])
        del flat["csv_options"]

    return flat


class DestinationSftpCsv(Destination):

    # ------------------------------------------------------------------
    # WRITE
    # ------------------------------------------------------------------
    def write(
        self,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage],
    ) -> Iterable[AirbyteMessage]:

        # FIX: flatten config BEFORE constructing SftpClient
        flat_config = _flatten_config(config)

        with SftpClient(**flat_config) as writer:

            # overwrite mode → delete previous file before writing new one
            for configured_stream in configured_catalog.streams:
                if configured_stream.destination_sync_mode == DestinationSyncMode.overwrite:
                    writer.delete(configured_stream.stream.name)

            try:
                for message in input_messages:
                    if message.type == Type.RECORD and message.record is not None:
                        writer.write(message.record.stream, message.record.data)
            finally:
                writer.flush_all()

        # No state emitted
        if False:
            yield

    # ------------------------------------------------------------------
    # CHECK CONNECTION
    # ------------------------------------------------------------------
    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        """
        Test SFTP connection: write + delete a temporary file.
        """

        try:
            stream = f"check_{uuid.uuid4().hex}"

            # FIX: flatten config BEFORE constructing SftpClient
            flat_config = _flatten_config(config)

            # Write test file
            with SftpClient(**flat_config) as writer:
                writer.write(stream, {"_airbyte_connection_check": True})

            # Delete test file (new session)
            with SftpClient(**flat_config) as writer:
                writer.delete(stream)

            return AirbyteConnectionStatus(status=Status.SUCCEEDED)

        except Exception as e:
            return AirbyteConnectionStatus(
                status=Status.FAILED,
                message=f"An exception occurred: {e}\nStacktrace:\n{traceback.format_exc()}",
            )