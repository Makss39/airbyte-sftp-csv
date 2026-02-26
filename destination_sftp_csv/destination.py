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


class DestinationSftpCsv(Destination):
    def write(
        self,
        config: Mapping[str, Any],
        configured_catalog: ConfiguredAirbyteCatalog,
        input_messages: Iterable[AirbyteMessage],
    ) -> Iterable[AirbyteMessage]:

        with SftpClient(**config) as writer:

            # Overwrite mode → supprimer fichiers existants avant la sync
            for configured_stream in configured_catalog.streams:
                if configured_stream.destination_sync_mode == DestinationSyncMode.overwrite:
                    writer.delete(configured_stream.stream.name)

            try:
                for message in input_messages:
                    if message.type == Type.RECORD and message.record is not None:
                        writer.write(message.record.stream, message.record.data)
            finally:
                writer.flush_all()

        # Aucun STATE n’est émis (Airbyte 2.x gère ça côté orchestrator)
        if False:
            yield

    # ------------------------------------------------------------------
    # CHECK CONNECTION
    # ------------------------------------------------------------------
    def check(self, logger: logging.Logger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        """
        Test SFTP connection: write + delete un fichier temporaire.
        """
        try:
            stream = f"check_{uuid.uuid4().hex}"

            # 1) Écrire le fichier de test
            with SftpClient(**config) as writer:
                writer.write(stream, {"_airbyte_connection_check": True})

            # 2) Supprimer le fichier dans une nouvelle session
            with SftpClient(**config) as writer:
                writer.delete(stream)

            return AirbyteConnectionStatus(status=Status.SUCCEEDED)

        except Exception as e:
            return AirbyteConnectionStatus(
                status=Status.FAILED,
                message=f"An exception occurred: {e}\nStacktrace:\n{traceback.format_exc()}",
            )