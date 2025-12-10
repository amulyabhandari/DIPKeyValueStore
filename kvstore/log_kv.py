import os
import struct
from typing import Optional, Dict, Tuple, BinaryIO, Union

HEADER_FORMAT = ">IIB"  # key_len (uint32), val_len (uint32), tomb (uint8)
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


class LogStructuredKV:
    """
    Simple log-structured key/value store with in-memory hash index.

    Disk format:
        [key_len:4][val_len:4][tomb:1][key][value]
    """

    def __init__(self, dirpath: str, max_active_size: int = 64 * 1024 * 1024):
        self.dirpath = dirpath
        os.makedirs(self.dirpath, exist_ok=True)

        self.max_active_size = max_active_size

        # index: key_bytes -> (filename, offset, record_size)
        self.index: Dict[bytes, Tuple[str, int, int]] = {}

        self.active_file: Optional[BinaryIO] = None
        self.active_filename: Optional[str] = None

        self._open_or_create()
        self._rebuild_index()

    # ------------------ public API ------------------ #

    def set(self, key: str, value: Union[bytes, str]) -> None:
        k = self._encode_key(key)
        v = self._encode_value(value)
        self._rotate_active_if_needed(len(k), len(v))

        offset = self.active_file.tell()
        tomb = 0
        header = struct.pack(HEADER_FORMAT, len(k), len(v), tomb)
        self.active_file.write(header)
        self.active_file.write(k)
        self.active_file.write(v)
        self.active_file.flush()

        record_size = HEADER_SIZE + len(k) + len(v)
        self.index[k] = (self.active_filename, offset, record_size)

    def get(self, key: str) -> Optional[bytes]:
        k = self._encode_key(key)
        entry = self.index.get(k)
        if entry is None:
            return None

        filename, offset, _ = entry
        path = os.path.join(self.dirpath, filename)
        with open(path, "rb") as f:
            f.seek(offset)
            key_len, val_len, tomb = struct.unpack(HEADER_FORMAT, f.read(HEADER_SIZE))
            if tomb == 1:
                return None
            f.read(key_len)  # skip key
            value = f.read(val_len)
            return value

    def delete(self, key: str) -> None:
        k = self._encode_key(key)
        if k not in self.index:
            pass

        self._rotate_active_if_needed(len(k), 0)
        offset = self.active_file.tell()
        tomb = 1
        header = struct.pack(HEADER_FORMAT, len(k), 0, tomb)
        self.active_file.write(header)
        self.active_file.write(k)
        self.active_file.flush()

        self.index.pop(k, None)

    def compact(self) -> None:
        compact_name = self._next_segment_name()
        compact_path = os.path.join(self.dirpath, compact_name)

        new_index: Dict[bytes, Tuple[str, int, int]] = {}

        with open(compact_path, "wb") as out_f:
            for k, (filename, offset, _) in self.index.items():
                path = os.path.join(self.dirpath, filename)
                with open(path, "rb") as in_f:
                    in_f.seek(offset)
                    key_len, val_len, tomb = struct.unpack(
                        HEADER_FORMAT, in_f.read(HEADER_SIZE)
                    )
                    assert tomb == 0
                    in_f.read(key_len)
                    v = in_f.read(val_len)

                new_offset = out_f.tell()
                header = struct.pack(HEADER_FORMAT, len(k), len(v), 0)
                out_f.write(header)
                out_f.write(k)
                out_f.write(v)
                record_size = HEADER_SIZE + len(k) + len(v)
                new_index[k] = (compact_name, new_offset, record_size)

        if self.active_file:
            self.active_file.close()

        for fname in os.listdir(self.dirpath):
            if fname.endswith(".log") and fname != compact_name:
                os.remove(os.path.join(self.dirpath, fname))

        self.active_filename = "active.log"
        self.active_file = open(os.path.join(self.dirpath, self.active_filename), "ab")

        self.index = new_index

    def close(self) -> None:
        if self.active_file:
            self.active_file.close()
            self.active_file = None

    # ------------------ internal helpers ------------------ #

    def _open_or_create(self) -> None:
        self.active_filename = "active.log"
        path = os.path.join(self.dirpath, self.active_filename)
        self.active_file = open(path, "ab+")

    def _rebuild_index(self) -> None:
        logs = sorted(
            fname for fname in os.listdir(self.dirpath)
            if fname.endswith(".log")
        )

        for fname in logs:
            path = os.path.join(self.dirpath, fname)
            with open(path, "rb") as f:
                offset = 0
                while True:
                    header = f.read(HEADER_SIZE)
                    if not header or len(header) < HEADER_SIZE:
                        break
                    key_len, val_len, tomb = struct.unpack(HEADER_FORMAT, header)
                    k = f.read(key_len)
                    if val_len > 0:
                        v = f.read(val_len)
                    else:
                        v = b""
                    record_size = HEADER_SIZE + key_len + val_len

                    if tomb == 1:
                        self.index.pop(k, None)
                    else:
                        self.index[k] = (fname, offset, record_size)

                    offset += record_size
                    f.seek(offset)

    def _rotate_active_if_needed(self, key_len: int, val_len: int) -> None:
        if self.active_file is None:
            self._open_or_create()
            return

        self.active_file.seek(0, os.SEEK_END)
        size = self.active_file.tell()
        if size + HEADER_SIZE + key_len + val_len > self.max_active_size:
            self.active_file.close()
            seg_name = self._next_segment_name()
            os.rename(
                os.path.join(self.dirpath, self.active_filename),
                os.path.join(self.dirpath, seg_name),
            )
            self.active_file = open(
                os.path.join(self.dirpath, self.active_filename), "ab"
            )

    def _next_segment_name(self) -> str:
        existing = [
            fname for fname in os.listdir(self.dirpath)
            if fname.startswith("segment-") and fname.endswith(".log")
        ]
        if not existing:
            return "segment-00001.log"

        numbers = [
            int(fname.split("-")[1].split(".")[0])
            for fname in existing
        ]
        n = max(numbers) + 1
        return f"segment-{n:05d}.log"

    @staticmethod
    def _encode_key(key: str) -> bytes:
        if isinstance(key, bytes):
            return key
        return key.encode("utf-8")

    @staticmethod
    def _encode_value(value: Union[bytes, str]) -> bytes:
        if isinstance(value, bytes):
            return value
        return value.encode("utf-8")
