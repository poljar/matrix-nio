# -*- coding: utf-8 -*-
# Copyright 2018 Zil0
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from builtins import bytes, int

from atomicwrites import atomic_write
from .._compat import package_installed
if package_installed("Crypto"):
    from Crypto import Random
    from Crypto.Cipher import AES
    from Crypto.Hash import HMAC, SHA256, SHA512
    from Crypto.Protocol.KDF import PBKDF2
    from Crypto.Util import Counter
    from unpaddedbase64 import decode_base64, encode_base64

HEADER = "-----BEGIN MEGOLM SESSION DATA-----"
FOOTER = "-----END MEGOLM SESSION DATA-----"


def encrypt_and_save(data: bytes, outfile: str, passphrase: str, count: int = 100000):
    """Encrypt keys data and write it to file.

    Args:
        data (bytes): The data to encrypt.
        outfile (str): The file the encrypted data will be written to.
        passphrase (str): The encryption passphrase.
        count (int): The round count used when deriving a key from the
            passphrase.
    Raises:
        FileNotFoundError if the path to the file did not exist.

    """
    encrypted_data = encrypt(data, passphrase, count=count)

    with atomic_write(outfile) as f:
        f.write(HEADER)
        f.write("\n")
        f.write(encrypted_data)
        f.write("\n")
        f.write(FOOTER)


def decrypt_and_read(infile: str, passphrase: str) -> bytes:
    """Decrypt keys data from file.

    Args:
        infile (str): The file the encrypted data will be written to.
        passphrase (str): The encryption passphrase.
    Returns:
        The decrypted data, as bytes.
    Raises:
        ValueError if something went wrong during decryption.
        FileNotFoundError if the file was not found.

    """
    with open(infile, "r") as f:
        encrypted_data = f.read()
    encrypted_data = encrypted_data.replace("\n", "")

    if not encrypted_data.startswith(HEADER) or not encrypted_data.endswith(FOOTER):
        raise ValueError("Wrong file format.")

    return decrypt(encrypted_data[len(HEADER) : -len(FOOTER)], passphrase)


def prf(passphrase, salt):
    """HMAC-SHA-512 pseudorandom function."""
    return HMAC.new(passphrase, salt, SHA512).digest()


def encrypt(data: bytes, passphrase: str, count: int = 100000):
    # 128 bits salt
    salt = Random.new().read(16)
    # 512 bits derived key
    derived_key = PBKDF2(passphrase, salt, 64, count, prf)  # type: ignore
    aes_key = derived_key[:32]
    hmac_key = derived_key[32:64]

    # 128 bits IV, which will be the initial value initial
    iv = int.from_bytes(Random.new().read(16), byteorder="big")
    # Set bit 63 to 0, as specified
    iv &= ~(1 << 63)
    ctr = Counter.new(128, initial_value=iv)
    cipher = AES.new(aes_key, AES.MODE_CTR, counter=ctr)
    encrypted_data = cipher.encrypt(data)

    payload = b"".join(
        (
            bytes([1]),  # Version
            salt,
            int.to_bytes(iv, length=16, byteorder="big"),
            # 32 bits big-endian round count
            int.to_bytes(count, length=4, byteorder="big"),
            encrypted_data,
        )
    )

    hmac = HMAC.new(hmac_key, payload, SHA256).digest()
    return encode_base64(payload + hmac)


def decrypt(encrypted_payload: str, passphrase: str):
    decoded_payload = decode_base64(encrypted_payload)

    version = decoded_payload[0]

    if isinstance(version, str):
        version = ord(version)

    if version != 1:
        raise ValueError("Unsupported export format version.")

    salt = decoded_payload[1:17]
    iv = int.from_bytes(decoded_payload[17:33], byteorder="big")
    count = int.from_bytes(decoded_payload[33:37], byteorder="big")
    encrypted_data = decoded_payload[37:-32]
    expected_hmac = decoded_payload[-32:]

    derived_key = PBKDF2(passphrase, salt, 64, count, prf)  # type: ignore
    aes_key = derived_key[:32]
    hmac_key = derived_key[32:64]

    hmac = HMAC.new(hmac_key, decoded_payload[:-32], SHA256).digest()

    if hmac != expected_hmac:
        raise ValueError("HMAC check failed for encrypted payload.")

    ctr = Counter.new(128, initial_value=iv)
    cipher = AES.new(aes_key, AES.MODE_CTR, counter=ctr)
    return cipher.decrypt(encrypted_data)
