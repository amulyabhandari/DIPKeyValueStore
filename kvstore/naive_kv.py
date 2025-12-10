import os
import struct
from typing import Optional, BinaryIO

HEADER_FORMAT = ">IIB"  # key_len (uint32), val_len (uint32), tomb (uint8)
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


class NaiveLogKV:
    """
    Naive log-structured key/value store without an in-memory index.

    Disk format:
        [key_len:4][val_len:4][tomb:1][key][value]

    - set: append record
    - get: scan whole file from start on every lookup
    - delete: append tombstone
    """

    def __init__(self, dirpath: str, filename: str = "naive.log"):
        self.dirpath = dirpath
        os.makedirs(self.dirpath, exist_ok=True)

        self.filename = filename
        self.filepath = os.path.join(self.dirpath, self.filename)

        # open in append mode, create if not exists
        self._file: BinaryIO = open(self.filepath, "ab+")

    def close(self) -> None:
        if self._file:
            self._file.close()
            self._file = None

    def set(self, key, value) -> None:
        k = self._encode_key(key)
        v = self._encode_value(value)

        # append record
        header = struct.pack(HEADER_FORMAT, len(k), len(v), 0)
        self._file.write(header)
        self._file.write(k)
        self._file.write(v)
        self._file.flush()

    def get(self, key) -> Optional[bytes]:
        """
        Naive O(N) read:
        - Scan file from beginning
        - Keep the latest value/tombstone for the given key
        """
        k = self._encode_key(key)

        # ensure we read from beginning
        self._file.flush()
        with open(self.filepath, "rb") as f:
            latest_value: Optional[bytes] = None
            deleted = False

            while True:
                header = f.read(HEADER_SIZE)
                if not header or len(header) < HEADER_SIZE:
                    break

                key_len, val_len, tomb = struct.unpack(HEADER_FORMAT, header)
                key_bytes = f.read(key_len)
                if val_len > 0:
                    value_bytes = f.read(val_len)
                else:
                    value_bytes = b""

                if key_bytes == k:
                    if tomb == 1:
                        deleted = True
                        latest_value = None
                    else:
                        deleted = False
                        latest_value = value_bytes

            if deleted or latest_value is None:
                return None
            return latest_value

    def delete(self, key) -> None:
        k = self._encode_key(key)
        # append tombstone with val_len = 0
        header = struct.pack(HEADER_FORMAT, len(k), 0, 1)
        self._file.write(header)
        self._file.write(k)
        self._file.flush()

    @staticmethod
    def _encode_key(key) -> bytes:
        if isinstance(key, bytes):
            return key
        return str(key).encode("utf-8")

    @staticmethod
    def _encode_value(value) -> bytes:
        if isinstance(value, bytes):
            return value
        return str(value).encode("utf-8")
