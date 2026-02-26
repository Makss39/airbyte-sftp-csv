import contextlib
import csv
from typing import Dict, List
from datetime import datetime

import smart_open
import paramiko
import errno


@contextlib.contextmanager
def sftp_client(host: str, port: int, username: str, password: str) -> paramiko.SFTPClient:
    """
    Context manager to manage SFTP connections.
    """
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=host,
        port=port,
        username=username,
        password=password,
        look_for_keys=False,
    )
    sftp = client.open_sftp()
    try:
        yield sftp
    finally:
        try:
            sftp.close()
        except Exception:
            pass
        client.close()


class SftpClient:
    """
    SFTP CSV writer with streaming + batch buffering.
    """

    def __init__(
        self,
        host: str,
        username: str,
        password: str,
        destination_path: str = "",
        filename: str = "",
        extension: str = ".csv",
        add_timestamp_to_filename: bool = False,
        port: int = 22,
        batch_size: int = 5000,
        separator: str = ",",
        encoding: str = "utf-8",
        quoting: str = "ALL",
        include_header: bool = True,
        line_terminator: str = "\n",
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.destination_path = destination_path.strip("/") if destination_path else ""
        self.filename = filename
        self.extension = extension if extension.startswith(".") else f".{extension}"
        self.add_timestamp_to_filename = add_timestamp_to_filename
        self.batch_size = batch_size
        self.separator = separator
        self.encoding = encoding
        self.include_header = include_header
        self.line_terminator = line_terminator

        # Convert quoting string to csv constant
        self.quoting = {
            "ALL": csv.QUOTE_ALL,
            "MINIMAL": csv.QUOTE_MINIMAL,
            "NONNUMERIC": csv.QUOTE_NONNUMERIC,
            "NONE": csv.QUOTE_NONE
        }.get(quoting.upper(), csv.QUOTE_ALL)

        # Buffers per stream
        self._buffers: Dict[str, List[Dict]] = {}
        self._files: Dict[str, smart_open.SmartOpenFile] = {}
        self._writers: Dict[str, csv.DictWriter] = {}
        self._headers_written: Dict[str, bool] = {}

    # Context manager support
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # ---------------------------------------------------------------------
    # PATHS
    # ---------------------------------------------------------------------
    def _remote_path(self, stream: str) -> str:
        if self.filename:
            filename = f"{self.filename}{self.extension}"
        else:
            filename = f"{stream}{self.extension}"

        if self.add_timestamp_to_filename:
            timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
            name, ext = filename.rsplit(".", 1)
            filename = f"{name}_{timestamp}.{ext}"

        if self.destination_path:
            return f"/{self.destination_path}/{filename}"
        return f"/{filename}"

    def _remote_uri(self, stream: str) -> str:
        return f"sftp://{self.username}:{self.password}@{self.host}:{self.port}{self._remote_path(stream)}"

    # ---------------------------------------------------------------------
    # STREAMING WRITE
    # ---------------------------------------------------------------------
    def _open_stream(self, stream: str, fieldnames):
        """
        Open SFTP file and initialize CSV writer.
        """
        uri = self._remote_uri(stream)
        fp = smart_open.open(
            uri,
            mode="w",
            encoding=self.encoding,
            transport_params={"connect_kwargs": {"look_for_keys": False}},
        )
        writer = csv.DictWriter(
            fp,
            fieldnames=fieldnames,
            delimiter=self.separator,
            quoting=self.quoting,
            lineterminator=self.line_terminator,
        )
        if self.include_header:
            writer.writeheader()
            self._headers_written[stream] = True
        else:
            self._headers_written[stream] = True
        self._files[stream] = fp
        self._writers[stream] = writer
        self._buffers[stream] = []

    def write(self, stream: str, record: Dict):
        """
        Add record to buffer; flush batch to SFTP if needed.
        """
        if stream not in self._writers:
            self._open_stream(stream, record.keys())

        self._buffers[stream].append(record)

        if len(self._buffers[stream]) >= self.batch_size:
            self._flush_stream(stream)

    def _flush_stream(self, stream: str):
        buffer = self._buffers.get(stream)
        if not buffer:
            return

        writer = self._writers[stream]
        writer.writerows(buffer)
        self._buffers[stream] = []

    def flush_all(self):
        """
        Flush remaining records and close all files.
        """
        for stream in list(self._files.keys()):
            # flush remaining buffer
            self._flush_stream(stream)
            # close file (triggers final write to SFTP)
            try:
                self._files[stream].close()
            finally:
                del self._files[stream]
                del self._writers[stream]
                del self._headers_written[stream]
                del self._buffers[stream]

    def close(self):
        self.flush_all()

    # ---------------------------------------------------------------------
    # DELETE
    # ---------------------------------------------------------------------
    def delete(self, stream: str):
        path = self._remote_path(stream)
        with sftp_client(self.host, self.port, self.username, self.password) as sftp:
            try:
                sftp.remove(path)
            except IOError as err:
                if err.errno != errno.ENOENT:
                    raise
