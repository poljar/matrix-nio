# -*- coding: utf-8 -*-

# Copyright © 2018 Damir Jelić <poljar@termina.org.uk>
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

from __future__ import unicode_literals

import json
import os
import sqlite3
# pylint: disable=redefined-builtin
from builtins import str
from collections import defaultdict
from functools import wraps
from typing import *

from logbook import Logger
from olm import (Account, InboundGroupSession, InboundSession, OlmAccountError,
                 OlmGroupSessionError, OlmPreKeyMessage, OlmSessionError,
                 OutboundGroupSession, OutboundSession, Session)

from .log import logger_group

logger = Logger('nio.encryption')
logger_group.add_logger(logger)


try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError


class OlmTrustError(Exception):
    pass


class EncryptionError(Exception):
    pass


class DeviceStore(object):
    def __init__(self, filename):
        # type: (str) -> None
        self._entries = []  # type: List[StoreEntry]
        self._filename = filename  # type: str

        self._load(filename)

    def __iter__(self):
        for entry in self._entries:
            yield OlmDevice(
                entry.user_id,
                entry.device_id,
                {entry.key_type: entry.key}
            )

    def _load(self, filename):
        # type: (str) -> None
        try:
            with open(filename, "r") as f:
                for line in f:
                    line = line.strip()

                    if not line or line.startswith("#"):
                        continue

                    entry = StoreEntry.from_line(line)

                    if not entry:
                        continue

                    self._entries.append(entry)
        except FileNotFoundError:
            pass

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
        with open(self._filename, "w") as f:
            for entry in self._entries:
                line = entry.to_line()
                f.write(line)

    @_save_store
    def add(self, device):
        # type: (OlmDevice) -> None
        new_entries = StoreEntry.from_olmdevice(device)
        self._entries += new_entries

        # Remove duplicate entries
        self._entries = list(set(self._entries))

        self._save()

    @_save_store
    def remove(self, device):
        # type: (OlmDevice) -> int
        removed = 0
        entries = StoreEntry.from_olmdevice(device)

        for entry in entries:
            if entry in self._entries:
                self._entries.remove(entry)
                removed += 1

        self._save()

        return removed

    def check(self, device):
        # type: (OlmDevice) -> bool
        return device in self


class StoreEntry(object):
    def __init__(self, user_id, device_id, key_type, key):
        # type: (str, str, str, str) -> None
        self.user_id = user_id
        self.device_id = device_id
        self.key_type = key_type
        self.key = key

    @classmethod
    def from_line(cls, line):
        # type: (str) -> Optional[StoreEntry]
        fields = line.split(' ')

        if len(fields) < 4:
            return None

        user_id, device_id, key_type, key = fields[:4]

        if key_type == "matrix-ed25519":
            return cls(user_id, device_id, "ed25519", key)
        else:
            return None

    @classmethod
    def from_olmdevice(cls, device_key):
        # type: (OlmDevice) -> List[StoreEntry]
        entries = []

        user_id = device_key.user_id
        device_id = device_key.device_id

        for key_type, key in device_key.keys.items():
            if key_type == "ed25519":
                entries.append(cls(user_id, device_id, "ed25519", key))

        return entries

    def to_line(self):
        # type: () -> str
        key_type = "matrix-{}".format(self.key_type)
        line = "{} {} {} {}\n".format(
            self.user_id,
            self.device_id,
            key_type,
            self.key
        )
        return line

    def __hash__(self):
        # type: () -> int
        return hash(str(self))

    def __str__(self):
        # type: () -> str
        key_type = "matrix-{}".format(self.key_type)
        line = "{} {} {} {}".format(
            self.user_id,
            self.device_id,
            key_type,
            self.key
        )
        return line

    def __eq__(self, value):
        # type: (object) -> bool
        if not isinstance(value, StoreEntry):
            return NotImplemented

        if (self.user_id == value.user_id
                and self.device_id == value.device_id
                and self.key_type == value.key_type
                and self.key == value.key):
            return True

        return False


class OlmDevice():
    def __init__(self, user_id, device_id, key_dict):
        # type: (str, str, Dict[str, str]) -> None
        self.user_id = user_id
        self.device_id = device_id
        self.keys = key_dict

    def __str__(self):
        # type: () -> str
        return "{} {} {}".format(
            self.user_id, self.device_id, self.keys["ed25519"])

    def __repr__(self):
        # type: () -> str
        return str(self)

    def __eq__(self, value):
        # type: (object) -> bool
        if not isinstance(value, OlmDevice):
            raise NotImplementedError

        try:
            if (self.user_id == value.user_id
                    and self.device_id == value.device_id
                    and self.keys["ed25519"] == value.keys["ed25519"]):
                return True
        except KeyError:
            pass

        return False


class OneTimeKey():
    def __init__(self, user_id, device_id, key):
        # type: (str, str, str) -> None
        self.user_id = user_id
        self.device_id = device_id
        self.key = key


class Olm():
    def __init__(
        self,
        user,                        # type: str
        device_id,                   # type: str
        session_path,                # type: str
    ):
        # type: (...) -> None
        self.user = user
        self.device_id = device_id
        self.session_path = session_path

        # List of group session ids that we shared with people
        self.shared_sessions = []  # type: List[str]

        # TODO the folowing dicts should probably be turned into classes with
        # nice interfaces for their operations
        # Dict containing devices of users that are members of encrypted rooms
        self.devices = {}  # type: Dict[str, List[OlmDevice]]

        # Dict of Olm sessions Dict[user_id, Dict[device_id, List[Session]]
        self.sessions = defaultdict(lambda: defaultdict(list)) \
            # type: DefaultDict[str, DefaultDict[str, List[Session]]]

        # Dict of inbound Megolm sessions
        # Dict[room_id, Dict[session_id, session]]
        self.inbound_group_sessions = defaultdict(dict) \
            # type: DefaultDict[str, Dict[str, InboundGroupSession]]

        # Dict of outbound Megolm sessions Dict[room_id]
        self.outbound_group_sessions = {} \
            # type: Dict[str, OutboundGroupSession]

        loaded = self.load()

        if not loaded:
            self.account = Account()
            self.save_account(True)

        # TODO we need a db for untrusted device as well as for seen devices.
        trust_file_path = "{}_{}.trusted_devices".format(user, device_id)
        self.trust_db = DeviceStore(os.path.join(
            session_path,
            trust_file_path
        ))

    def _create_session(self, sender, sender_key, message):
        logger.info("Creating Inbound session for {}".format(sender))
        session = InboundSession(self.account, message, sender_key)
        logger.info("Created Inbound session for {}".format(sender))
        self.account.remove_one_time_keys(session)
        self.save_account()

        return session

    def verify_device(self, device):
        if device in self.trust_db:
            return False

        self.trust_db.add(device)
        return True

    def unverify_device(self, device):
        self.trust_db.remove(device)

    def create_session(self, user_id, device_id, one_time_key):
        id_key = None

        logger.info("Creating Outbound for {} and device {}".format(
            user_id, device_id))

        for user, keys in self.devices.items():
            if user != user_id:
                continue

            for key in keys:
                if key.device_id == device_id:
                    id_key = key.keys["curve25519"]
                    break

        if not id_key:
            logger.error("Identity key for device {} not found".format(
                device_id))
            # TODO raise error here

        logger.info("Found identity key for device {}".format(device_id))
        session = OutboundSession(self.account, id_key, one_time_key)
        self.save_account()
        self.sessions[user_id][device_id].append(session)
        self.save_session(user_id, device_id, session)
        logger.info("Created OutboundSession for device {}".format(device_id))

    def create_group_session(self, room_id, session_id, session_key):
        logger.info("Creating inbound group session for {}".format(room_id))
        session = InboundGroupSession(session_key)
        self.inbound_group_sessions[room_id][session_id] = session
        self.save_inbound_group_session(room_id, session)
        logger.info("Created inbound group session for {}".format(room_id))

    def create_outbound_group_session(self, room_id):
        logger.info("Creating outbound group session for {}".format(room_id))
        session = OutboundGroupSession()
        self.outbound_group_sessions[room_id] = session
        self.create_group_session(room_id, session.id, session.session_key)
        logger.info("Created outbound group session for {}".format(room_id))

    def get_missing_sessions(self, users):
        # type: (List[str]) -> Dict[str, Dict[str, str]]
        missing = {}

        for user in users:
            devices = []

            for key in self.devices[user]:
                # we don't need a session for our own device, skip it
                if key.device_id == self.device_id:
                    continue

                if not self.sessions[user][key.device_id]:
                    logger.warn("Missing session for device {}".format(
                        key.device_id))
                    devices.append(key.device_id)

            if devices:
                missing[user] = {device: "signed_curve25519" for
                                 device in devices}

        return missing

    def decrypt(self, sender, sender_key, message):
        plaintext = None

        for device_id, session_list in self.sessions[sender].items():
            for session in session_list:
                try:
                    if isinstance(message, OlmPreKeyMessage):
                        if not session.matches(message):
                            continue

                    logger.info("Trying to decrypt olm message using existing "
                                "session for {} and device {}".format(
                                    sender,
                                    device_id
                                ))

                    plaintext = session.decrypt(message)
                    parsed_plaintext = json.loads(plaintext, encoding='utf-8')

                    logger.info("Succesfully decrypted olm message "
                                "using existing session")
                    return parsed_plaintext
                except OlmSessionError as e:
                    logger.warn("Error decrypting olm message from {} "
                                "and device {}: {}".format(
                                    sender,
                                    device_id,
                                    str(e)
                                ))
                    pass

        try:
            session = self._create_session(sender, sender_key, message)
        except OlmSessionError:
            return None

        try:
            plaintext = session.decrypt(message)
            parsed_plaintext = json.loads(plaintext, encoding='utf-8')

            device_id = parsed_plaintext["sender_device"]
            self.sessions[sender][device_id].append(session)
            self.save_session(sender, device_id, session)
            return parsed_plaintext
        except OlmSessionError:
            return None

    def group_encrypt(
        self,
        room_id,         # type: str
        plaintext_dict,  # type: Dict[str, str]
        own_id,          # type: str
        users            # type: str
    ):
        # type: (...) -> Tuple[Dict[str, str], Optional[Dict[Any, Any]]]
        plaintext_dict["room_id"] = room_id
        to_device_dict = None

        if room_id not in self.outbound_group_sessions:
            self.create_outbound_group_session(room_id)

        if (self.outbound_group_sessions[room_id].id
                not in self.shared_sessions):
            to_device_dict = self.share_group_session(room_id, own_id, users)
            self.shared_sessions.append(
                self.outbound_group_sessions[room_id].id
            )

        session = self.outbound_group_sessions[room_id]

        ciphertext = session.encrypt(Olm._to_json(plaintext_dict))

        payload_dict = {
            "algorithm": "m.megolm.v1.aes-sha2",
            "sender_key": self.account.identity_keys()["curve25519"],
            "ciphertext": ciphertext,
            "session_id": session.id,
            "device_id": self.device_id
        }

        return payload_dict, to_device_dict

    def group_decrypt(self, room_id, session_id, ciphertext):
        if session_id not in self.inbound_group_sessions[room_id]:
            return None

        session = self.inbound_group_sessions[room_id][session_id]
        try:
            plaintext = session.decrypt(ciphertext)
        except OlmGroupSessionError:
            return None

        return plaintext

    def share_group_session(self, room_id, own_id, users):
        group_session = self.outbound_group_sessions[room_id]

        key_content = {
            "algorithm": "m.megolm.v1.aes-sha2",
            "room_id": room_id,
            "session_id": group_session.id,
            "session_key": group_session.session_key,
            "chain_index": group_session.message_index
        }

        payload_dict = {
            "type": "m.room_key",
            "content": key_content,
            # TODO we don't have the user_id in the Olm class
            "sender": own_id,
            "sender_device": self.device_id,
            "keys": {
                "ed25519": self.account.identity_keys()["ed25519"]
            }
        }

        to_device_dict = {
            "messages": {}
        }

        for user in users:
            if user not in self.devices:
                continue

            for key in self.devices[user]:
                if key.device_id == self.device_id:
                    continue

                if not self.sessions[user][key.device_id]:
                    continue

                if key not in self.trust_db:
                    raise OlmTrustError

                device_payload_dict = payload_dict.copy()
                # TODO sort the sessions
                session = self.sessions[user][key.device_id][0]
                device_payload_dict["recipient"] = user
                device_payload_dict["recipient_keys"] = {
                    "ed25519": key.keys["ed25519"]
                }

                olm_message = session.encrypt(
                    Olm._to_json(device_payload_dict)
                )

                olm_dict = {
                    "algorithm": "m.olm.v1.curve25519-aes-sha2",
                    "sender_key": self.account.identity_keys()["curve25519"],
                    "ciphertext": {
                        key.keys["curve25519"]: {
                            "type": (0 if isinstance(
                                olm_message,
                                OlmPreKeyMessage
                            ) else 1),
                            "body": olm_message.ciphertext
                        }
                    }
                }

                if user not in to_device_dict["messages"]:
                    to_device_dict["messages"][user] = {}

                to_device_dict["messages"][user][key.device_id] = olm_dict

        return to_device_dict

    def load(self):
        # type: () -> bool

        db_file = "{}_{}.db".format(self.user, self.device_id)
        db_path = os.path.join(self.session_path, db_file)

        self.database = sqlite3.connect(db_path)
        new = Olm._check_db_tables(self.database)

        if new:
            return False

        cursor = self.database.cursor()

        cursor.execute(
            "select pickle from olmaccount where user = ?",
            (self.user,)
        )
        row = cursor.fetchone()
        account_pickle = row[0]

        cursor.execute("select user, device_id, pickle from olmsessions")
        db_sessions = cursor.fetchall()

        cursor.execute("select room_id, pickle from inbound_group_sessions")
        db_inbound_group_sessions = cursor.fetchall()

        cursor.close()

        try:
            self.account = Account.from_pickle(account_pickle)

            for db_session in db_sessions:
                s = Session.from_pickle(db_session[2])
                self.sessions[db_session[0]][db_session[1]].append(s)

            for db_session in db_inbound_group_sessions:
                s = InboundGroupSession.from_pickle(db_session[1])
                self.inbound_group_sessions[db_session[0]][s.id] = s

        except (OlmAccountError, OlmSessionError) as error:
            raise EncryptionError(error)

        return True

    def save_session(self, user, device_id, session):
        cursor = self.database.cursor()

        cursor.execute("insert into olmsessions values(?,?,?,?)",
                       (user, device_id, session.id, session.pickle()))

        self.database.commit()

        cursor.close()

    def save_inbound_group_session(self, room_id, session):
        cursor = self.database.cursor()

        cursor.execute("insert into inbound_group_sessions values(?,?,?)",
                       (room_id, session.id, session.pickle()))

        self.database.commit()

        cursor.close()

    def save_account(self, new=False):
        cursor = self.database.cursor()

        if new:
            cursor.execute("insert into olmaccount values (?,?)",
                           (self.user, self.account.pickle()))
        else:
            cursor.execute("update olmaccount set pickle=? where user = ?",
                           (self.account.pickle(), self.user))

        self.database.commit()
        cursor.close()

    @staticmethod
    def _check_db_tables(database):
        # type: (sqlite3.Connection) -> bool
        new = False
        cursor = database.cursor()
        cursor.execute("""select name from sqlite_master where type='table'
                          and name='olmaccount'""")
        if not cursor.fetchone():
            cursor.execute("create table olmaccount (user text, pickle text)")
            database.commit()
            new = True

        cursor.execute("""select name from sqlite_master where type='table'
                          and name='olmsessions'""")
        if not cursor.fetchone():
            cursor.execute("""create table olmsessions (user text,
                              device_id text, session_id text, pickle text)""")
            database.commit()
            new = True

        cursor.execute("""select name from sqlite_master where type='table'
                          and name='inbound_group_sessions'""")
        if not cursor.fetchone():
            cursor.execute("""create table inbound_group_sessions
                              (room_id text, session_id text, pickle text)""")
            database.commit()
            new = True

        cursor.close()
        return new

    def sign_json(self, json_dict):
        # type: (Dict[Any, Any]) -> str
        signature = self.account.sign(self._to_json(json_dict))
        return signature

    @staticmethod
    def _to_json(json_dict):
        # type: (Dict[Any, Any]) -> str
        return json.dumps(
            json_dict,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True
        )

    def mark_keys_as_published(self):
        self.account.mark_keys_as_published()