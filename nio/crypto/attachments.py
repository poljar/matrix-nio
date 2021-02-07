# -*- coding: utf-8 -*-

# Copyright 2018 Zil0
# Copyright © 2019 Damir Jelić <poljar@termina.org.uk>
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This function is part of the matrix-python-sdk and is distributed
# under the APACHE 2.0 licence.

"""Matrix encryption algorithms for file uploads."""

from binascii import Error as BinAsciiError
from typing import Any, Dict, Generator, Iterable, Tuple, Union

import unpaddedbase64
from .._compat import package_installed
if package_installed("Crypto"):
    from Crypto import Random
    from Crypto.Cipher import AES
    from Crypto.Hash import SHA256
    from Crypto.Util import Counter

from ..exceptions import EncryptionError

DataT = Union[bytes, Iterable[bytes]]

if package_installed("Crypto"):
    def decrypt_attachment(ciphertext: bytes, key: str, hash: str, iv: str):
        """Decrypt an encrypted attachment.

        Args:
            ciphertext (bytes): The data to decrypt.
            key (str): AES_CTR JWK key object.
            hash (str): Base64 encoded SHA-256 hash of the ciphertext.
            iv (str): Base64 encoded 16 byte AES-CTR IV.
        Returns:
            The plaintext bytes.
        Raises:
            EncryptionError if the integrity check fails.


        """
        expected_hash = unpaddedbase64.decode_base64(hash)

        h = SHA256.new()
        h.update(ciphertext)

        if h.digest() != expected_hash:
            raise EncryptionError("Mismatched SHA-256 digest.")

        try:
            byte_key: bytes = unpaddedbase64.decode_base64(key)
        except (BinAsciiError, TypeError):
            raise EncryptionError("Error decoding key.")

        try:
            # Drop last 8 bytes, which are 0
            byte_iv: bytes = unpaddedbase64.decode_base64(iv)[:8]
        except (BinAsciiError, TypeError):
            raise EncryptionError("Error decoding initial values.")

        ctr = Counter.new(64, prefix=byte_iv, initial_value=0)

        try:
            cipher = AES.new(byte_key, AES.MODE_CTR, counter=ctr)
        except ValueError as e:
            raise EncryptionError(e)

        return cipher.decrypt(ciphertext)


    def encrypt_attachment(plaintext: bytes) -> Tuple[bytes, Dict[str, Any]]:
        """Encrypt data in order to send it as an encrypted attachment.

        Args:
            data (bytes): The data to encrypt.

        Returns:
            A tuple with the encrypted bytes and a dict containing the info needed
            to decrypt data. See ``encrypted_attachment_generator()`` for the keys.
        """

        values = list(encrypted_attachment_generator(plaintext))
        encrytped_bytes: bytes = b"".join(values[:-1])  # type: ignore
        keys: Dict[str, Any] = values[-1]  # type: ignore
        return (encrytped_bytes, keys)


    def encrypted_attachment_generator(
        data: DataT,
    ) -> Generator[Union[bytes, Dict[str, Any]], None, None]:
        """Generator to encrypt data in order to send it as an encrypted
        attachment.

        Unlike ``encrypt_attachment()``, this function lazily encrypts and yields
        data, thus it can be used to encrypt large files without fully loading them
        into memory if an iterable of bytes is passed as data.

        Args:
            data (bytes/Iterable[bytes]): The data to encrypt.

        Yields:
            The encrypted bytes for each chunk of data.
            The last yielded value will be a dict containing the info needed to
            decrypt data. The keys are:
            | key: AES-CTR JWK key object.
            | iv: Base64 encoded 16 byte AES-CTR IV.
            | hashes.sha256: Base64 encoded SHA-256 hash of the ciphertext.
        """

        key = Random.new().read(32)
        # 8 bytes IV
        iv = Random.new().read(8)
        # 8 bytes counter, prefixed by the IV
        ctr = Counter.new(64, prefix=iv, initial_value=0)

        cipher = AES.new(key, AES.MODE_CTR, counter=ctr)
        sha256 = SHA256.new()

        if isinstance(data, bytes):
            data = [data]

        for chunk in data:
            encrypted_chunk = cipher.encrypt(chunk)  # in executor
            sha256.update(encrypted_chunk)  # in executor
            yield encrypted_chunk

        yield _get_decryption_info_dict(key, iv, sha256)


    def _get_decryption_info_dict(
        key: bytes, iv: bytes, sha256: SHA256.SHA256Hash
    ) -> Dict[str, Any]:
        json_web_key = {
            "kty": "oct",
            "alg": "A256CTR",
            "ext": True,
            "k": unpaddedbase64.encode_base64(key, urlsafe=True),
            "key_ops": ["encrypt", "decrypt"],
        }

        return {
            "v": "v2",
            "key": json_web_key,
            # Send IV concatenated with counter
            "iv": unpaddedbase64.encode_base64(iv + b"\x00" * 8),
            "hashes": {"sha256": unpaddedbase64.encode_base64(sha256.digest()),},
        }
