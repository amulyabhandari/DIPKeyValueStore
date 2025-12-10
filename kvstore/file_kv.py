import os
from typing import Optional


class FilePerKeyKV:
    """
    Extremely simple key/value store:
    - One file per key in a directory.
    - set: write the whole file.
    - get: read the file.
    - delete: remove the file.

    This is intentionally naive and uses the filesystem directly
    without any log-structured optimization.
    """

    def __init__(self, dirpath: str):
        self.dirpath = dirpath
        os.makedirs(self.dirpath, exist_ok=True)

    def _key_path(self, key: str) -> str:
        # very simple mapping: file name is the key itself
        # (safe because in benchmarks we use keys like "key_123").
        return os.path.join(self.dirpath, key)

    def set(self, key: str, value) -> None:
        path = self._key_path(key)
        if isinstance(value, bytes):
            data = value
        else:
            data = str(value).encode("utf-8")
        with open(path, "wb") as f:
            f.write(data)
            f.flush()

    def get(self, key: str) -> Optional[bytes]:
        path = self._key_path(key)
        if not os.path.exists(path):
            return None
        with open(path, "rb") as f:
            return f.read()

    def delete(self, key: str) -> None:
        path = self._key_path(key)
        if os.path.exists(path):
            os.remove(path)

    def close(self) -> None:
        # added for compatibility with LogStructuredKV interface
        pass