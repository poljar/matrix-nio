from functools import wraps
from typing import DefaultDict, Dict, Iterator, List, Optional

from atomicwrites import atomic_write

from . import logger
from ..crypto import OlmDevice
from ..exceptions import OlmTrustError

try:
    FileNotFoundError  # type: ignore
except NameError:  # pragma: no cover
    FileNotFoundError = IOError


class Key(object):
    def __init__(self, user_id, device_id, key):
        # type: (str, str, str) -> None
        self.user_id = user_id
        self.device_id = device_id
        self.key = key

    @classmethod
    def from_line(cls, line):
        # type: (str) -> Optional[Key]
        fields = line.split(" ")

        if len(fields) < 4:
            return None

        user_id, device_id, key_type, key = fields[:4]

        if key_type == "matrix-ed25519":
            return Ed25519Key(user_id.strip(), device_id.strip(), key.strip())
        else:
            return None

    def to_line(self):
        # type: () -> str
        key_type = ""

        if isinstance(self, Ed25519Key):
            key_type = "matrix-ed25519"
        else:  # pragma: no cover
            raise NotImplementedError(
                "Invalid key type {}".format(type(self.key))
            )

        line = "{} {} {} {}\n".format(
            self.user_id, self.device_id, key_type, str(self.key)
        )
        return line

    @classmethod
    def from_olmdevice(cls, device):
        # type: (OlmDevice) -> Ed25519Key
        user_id = device.user_id
        device_id = device.id
        return Ed25519Key(user_id, device_id, device.ed25519)


class Ed25519Key(Key):
    def __eq__(self, value):
        # type: (object) -> bool
        if not isinstance(value, Ed25519Key):
            return NotImplemented

        if (
            self.user_id == value.user_id
            and self.device_id == value.device_id
            and self.key == value.key
        ):
            return True

        return False


class KeyStore(object):
    def __init__(self, filename):
        # type: (str) -> None
        self._entries = []  # type: List[Key]
        self._filename = filename  # type: str

        self._load(filename)

    def __iter__(self):
        # type: () -> Iterator[Key]
        for entry in self._entries:
            yield entry

    def __repr__(self):
        # type: () -> str
        return "KeyStore object, file: {}".format(self._filename)

    def _load(self, filename):
        # type: (str) -> None
        try:
            with open(filename, "r") as f:
                for line in f:
                    line = line.strip()

                    if not line or line.startswith("#"):
                        continue

                    entry = Key.from_line(line)

                    if not entry:
                        continue

                    self._entries.append(entry)
        except FileNotFoundError:
            pass

    def get_key(self, user_id, device_id):
        # type: (str, str) -> Optional[Key]
        for entry in self._entries:
            if user_id == entry.user_id and device_id == entry.device_id:
                return entry

        return None

    def _save_store(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            self = args[0]
            ret = f(*args, **kwargs)
            self._save()
            return ret

        return decorated

    def _save(self):
        # type: () -> None
        with atomic_write(self._filename, overwrite=True) as f:
            for entry in self._entries:
                line = entry.to_line()
                f.write(line)

    @_save_store
    def add(self, key):
        # type: (Key) -> bool
        existing_key = self.get_key(key.user_id, key.device_id)

        if existing_key:
            if (
                existing_key.user_id == key.user_id
                and existing_key.device_id == key.device_id
                and type(existing_key) is type(key)
            ):
                if existing_key.key != key.key:
                    message = (
                        "Error: adding existing device to trust store "
                        "with mismatching fingerprint {} {}".format(
                            key.key, existing_key.key
                        )
                    )
                    logger.error(message)
                    raise OlmTrustError(message)

        self._entries.append(key)
        return True

    @_save_store
    def remove(self, key):
        # type: (Key) -> bool
        if key in self._entries:
            self._entries.remove(key)
            return True

        return False

    def check(self, key):
        # type: (Key) -> bool
        return key in self._entries
