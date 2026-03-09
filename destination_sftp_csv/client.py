import contextlib
import csv
from typing import Dict, List
from datetime import datetime

import smart_open
import paramiko
import errno
import pandas as pd
from io import StringIO


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
    SFTP CSV writer with two modes:
      - direct: streaming + batch buffering (csv.DictWriter)
      - pandas: accumulate rows per stream in a DataFrame, then single CSV upload at the end

    Common options are honored in both modes where applicable.
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
        separator: str = "",
        encoding: str = "utf-8",
        quoting: str = "ALL",
        include_header: bool = True,
        line_terminator: str = "\n",
        # --- NEW ---
        extraction_mode: str = "direct",        # "direct" | "pandas"
        quotechar: str = '"',                   # applicable to both modes
        na_rep: str = "",                       # pandas: how to represent NaN/None
        # escapechar could be added if needed
    ):
        # connection / naming
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.destination_path = destination_path.strip("/") if destination_path else ""
        self.filename = filename
        self.extension = extension if extension.startswith(".") else f".{extension}"
        self.add_timestamp_to_filename = add_timestamp_to_filename

        # io settings
        self.batch_size = batch_size
        self.separator = separator
        self.encoding = encoding
        self.include_header = include_header
        self.line_terminator = line_terminator
        self.quotechar = quotechar
        self.na_rep = na_rep

        # mode
        self.extraction_mode = (extraction_mode or "direct").lower()
        if self.extraction_mode not in ("direct", "pandas", "fixed"):
            raise ValueError("extraction_mode must be 'direct', 'pandas' or 'fixed'")
        if self.extraction_mode not in ("direct", "pandas"):
            raise ValueError("extraction_mode must be 'direct' or 'pandas'")
        # quoting mapping
        self.quoting = {
            "ALL": csv.QUOTE_ALL,
            "MINIMAL": csv.QUOTE_MINIMAL,
            "NONNUMERIC": csv.QUOTE_NONNUMERIC,
            "NONE": csv.QUOTE_NONE,
        }.get(quoting.upper(), csv.QUOTE_ALL)

        # Buffers per stream
        self._buffers: Dict[str, List[Dict]] = {}
        self._files: Dict[str, smart_open.SmartOpenFile] = {}
        self._writers: Dict[str, csv.DictWriter] = {}
        self._headers_written: Dict[str, bool] = {}

        # Pandas accumulators
        self._dataframes: Dict[str, pd.DataFrame] = {}

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
    # STREAMING WRITE (direct mode)
    # ---------------------------------------------------------------------
    def _open_stream(self, stream: str, fieldnames):
        """
        Open SFTP file and initialize CSV writer.
        (direct mode only)
        """
        uri = self._remote_uri(stream)
        fp = smart_open.open(
            uri,
            mode="w",
            encoding=self.encoding,
            transport_params={"connect_kwargs": {"look_for_keys": False}},
        )

        # --- FIXED WIDTH / NO DELIMITER MODE ---
        if self.extraction_mode == "fixed":
            self._files[stream] = fp
            self._writers[stream] = None
            self._buffers[stream] = []
            self._headers_written[stream] = True  # no header
            self._fixed_fields = fieldnames
            return

        # --- NORMAL CSV MODE ---
        writer = csv.DictWriter(
            fp,
            fieldnames=fieldnames,
            delimiter=self.separator,
            quoting=self.quoting,
            lineterminator=self.line_terminator,
            quotechar=self.quotechar,
        )
        if self.include_header:
            writer.writeheader()
            self._headers_written[stream] = True
        else:
            self._headers_written[stream] = True

        self._files[stream] = fp
        self._writers[stream] = writer
        self._buffers[stream] = []

    def _write_direct(self, stream: str, record: Dict):
        if stream not in self._writers:
            self._open_stream(stream, list(record.keys()))

        # --- FIXED WIDTH MODE ---
        if self.extraction_mode == "fixed":
            fp = self._files[stream]
            line = "".join(str(v) for v in record.values()) + self.line_terminator
            fp.write(line)
            return

        # --- NORMAL CSV MODE ---
        self._buffers[stream].append(record)
        if len(self._buffers[stream]) >= self.batch_size:
            self._flush_stream(stream)

    def _flush_stream(self, stream: str):
       # FIXED: no-op
        if self.extraction_mode == "fixed":
            self._buffers[stream] = []
            return

        buffer = self._buffers.get(stream)
        if not buffer:
            return
        writer = self._writers[stream]
        writer.writerows(buffer)
        self._buffers[stream] = []

    # ---------------------------------------------------------------------
    # PANDAS MODE
    # ---------------------------------------------------------------------
    def _write_pandas(self, stream: str, record: Dict):
        df_row = pd.DataFrame([record])
        if stream not in self._dataframes:
            self._dataframes[stream] = df_row
        else:
            # concat to preserve union of columns if schema drifts
            self._dataframes[stream] = pd.concat(
                [self._dataframes[stream], df_row],
                ignore_index=True,
            )

    def _flush_all_pandas(self):
        for stream, df in self._dataframes.items():
            # Convert DataFrame to CSV string using pandas
            csv_buffer = StringIO()
            # NB: pandas uses "lineterminator" (no underscore)
            df.to_csv(
                csv_buffer,
                sep=self.separator,
                encoding=self.encoding,
                index=False,
                header=self.include_header,
                lineterminator=self.line_terminator,
                quoting=self.quoting,
                quotechar=self.quotechar,
                na_rep=self.na_rep,
            )
            uri = self._remote_uri(stream)
            with smart_open.open(
                uri,
                mode="w",
                encoding=self.encoding,
                transport_params={"connect_kwargs": {"look_for_keys": False}},
            ) as fp:
                fp.write(csv_buffer.getvalue())

        self._dataframes.clear()

    # ---------------------------------------------------------------------
    # PUBLIC API
    # ---------------------------------------------------------------------
    def write(self, stream: str, record: Dict):
        """
        Add record to buffer; behavior depends on extraction_mode.
        """
        if self.extraction_mode == "pandas":
            self._write_pandas(stream, record)
        else:
            self._write_direct(stream, record)

    def flush_all(self):
        """
        Flush remaining records and close all files (direct) or
        materialize DataFrames and upload (pandas).
        """

        #PANDAS
        if self.extraction_mode == "pandas":
            self._flush_all_pandas()
            return

        # FIXED-WIDTH
        if self.extraction_mode == "fixed":
            for stream in list(self._files.keys()):
                try:
                    self._files[stream].close()
                finally:
                    del self._files[stream]
                    del self._buffers[stream]
                    del self._headers_written[stream]
            return

        # direct mode: flush and close per stream
        for stream in list(self._files.keys()):
            self._flush_stream(stream)
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

        """
        Delete single file OR all check_* test files.
        """
        remote_dir = "/" + self.destination_path if self.destination_path else "/"

        # Special case: connection test → purge all check_* files
        if stream.startswith("check_"):
            with sftp_client(self.host, self.port, self.username, self.password) as sftp:
                try:
                    files = sftp.listdir(remote_dir)
                except IOError:
                    return

                for f in files:
                    if f.startswith("check_"):
                        try:
                            sftp.remove(remote_dir.rstrip("/") + "/" + f)
                        except Exception:
                            pass
            return

        """
        Remove remote file if exists. Works the same in both modes.
        """
        path = self._remote_path(stream)
        with sftp_client(self.host, self.port, self.username, self.password) as sftp:
            try:
                sftp.remove(path)
            except IOError as err:
                if err.errno != errno.ENOENT:
                    raise