# Copyright © 2018, 2019 Damir Jelić <poljar@termina.org.uk>
# Copyright © 2019 miruka <miruka@disroot.org>
#
# Permission to use, copy, modify, and/or distribute this software for
# any purpose with or without fee is hereby granted, provided that the
# above copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
# SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
# RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
# CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
# CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

"""Matrix async encryption/decryption functions for file uploads."""

import asyncio
import io
from functools import partial
from pathlib import Path
from typing import Any, AsyncGenerator, AsyncIterable, Dict, Iterable, Union

import aiofiles
from aiofiles.threadpool.binary import AsyncBufferedReader
from .._compat import package_installed
if package_installed("Crypto"):
    from Crypto import Random  # nosec
    from Crypto.Cipher import AES  # nosec
    from Crypto.Hash import SHA256  # nosec
    from Crypto.Util import Counter  # nosec

    from .attachments import _get_decryption_info_dict

AsyncDataT = Union[
    str,
    Path,
    bytes,
    Iterable[bytes],
    AsyncIterable[bytes],
    io.BufferedIOBase,
    AsyncBufferedReader,
]

_EncryptedReturnT = AsyncGenerator[Union[bytes, Dict[str, Any]], None]


async def async_encrypt_attachment(data: AsyncDataT) -> _EncryptedReturnT:
    """Async generator to encrypt data in order to send it as an encrypted
    attachment.

    This function lazily encrypts and yields data, thus it can be used to
    encrypt large files without fully loading them into memory if an iterable
    or async iterable of bytes is passed as data.

    Args:
        data (str/Path/bytes/Iterable[bytes]/AsyncIterable[bytes]/
        io.BufferedIOBase/AsyncBufferedReader): The data to encrypt.
            Passing a path string, Path, async iterable or aiofiles open
            binary file object allows the file data to be read in an
            asynchronous and lazy (without reading the entire file into
            memory) way.
            Passing a non-async iterable or standard open binary file
            object will still allow the data to be read lazily, but
            not asynchronously.

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

    loop = asyncio.get_event_loop()

    async for chunk in async_generator_from_data(data):
        update_crypt = partial(cipher.encrypt, chunk)
        crypt_chunk  = await loop.run_in_executor(None, update_crypt)

        update_hash = partial(sha256.update, crypt_chunk)
        await loop.run_in_executor(None, update_hash)

        yield crypt_chunk

    yield _get_decryption_info_dict(key, iv, sha256)


async def async_generator_from_data(
    data:       AsyncDataT,
    chunk_size: int = 4 * 1024,
) -> AsyncGenerator[bytes, None]:

    aio_opened = False
    if isinstance(data, (str, Path)):
        data       = await aiofiles.open(data, "rb")
        aio_opened = True

    ###

    if isinstance(data, bytes):
        chunks = (
            data[i : i + chunk_size] for i in range(0, len(data), chunk_size)
        )
        for chunk in chunks:
            yield chunk

    # Test if data is a file obj first, since it's considered Iterable too
    elif isinstance(data, io.BufferedIOBase):
       while True:
            chunk = data.read(chunk_size)
            if not chunk:
                return
            yield chunk

    elif isinstance(data, AsyncBufferedReader):
        while True:
            chunk = await data.read(chunk_size)
            if not chunk:
                break
            yield chunk

        if aio_opened:
            await data.close()

    elif isinstance(data, Iterable):
        for chunk in data:  # type: ignore
            yield chunk

    elif isinstance(data, AsyncIterable):
        async for chunk in data:
            yield chunk

    else:
        raise TypeError(f"Unknown type for data: {data!r}")
