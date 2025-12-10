import os
from typing import Optional


class FilePerKeyKV:
    """
    Simple filesystem-based key/value store.

    - One file per key.
    - Direct path lookup for reads and deletes.
    - Normal writes without forced fsync.
    """

    def __init__(self, dirpath: str):
        self.dirpath = dirpath
        os.makedirs(self.dirpath, exist_ok=True)

    def _key_path(self, key: str) -> str:
        return os.path.join(self.dirpath, key)

    def set(self, key: str, value) -> None:
        path = self._key_path(key)

        if isinstance(value, bytes):
            data = value
        else:
            data = str(value).encode("utf-8")

        # simple write (fast, no fsync)
        with open(path, "wb") as f:
            f.write(data)

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

    def close(self):
        pass
