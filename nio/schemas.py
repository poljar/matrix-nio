# -*- coding: utf-8 -*-

# Copyright © 2018 Damir Jelić <poljar@termina.org.uk>
# Copyright © 2020 Famedly GmbH
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

import re

from jsonschema import Draft4Validator, FormatChecker, validators

RoomRegex = "^!.+:.+$"
UserIdRegex = "^@[a-z0-9._=/]+:.+$"
EventTypeRegex = r"^.+\..+"
Base64Regex = r"[^-A-Za-z0-9+/=]|=[^=]|={3,}$"
KeyRegex = r"(ed25519|curve25519):.+"
SignedCurveRegex = r"(signed_curve25519|curve25519):.+"


def extend_with_default(validator_class):
    validate_properties = validator_class.VALIDATORS["properties"]

    def set_defaults(validator, properties, instance, schema):
        for property, subschema in properties.items():
            if "default" in subschema:
                instance.setdefault(property, subschema["default"])

        for error in validate_properties(
            validator, properties, instance, schema
        ):
            yield error

    return validators.extend(validator_class, {"properties": set_defaults})


Validator = extend_with_default(Draft4Validator)


@FormatChecker.cls_checks("user_id", ValueError)
def check_user_id(value):
    # type: (str) -> bool
    if re.match(UserIdRegex, value) is None:
        raise ValueError(f"UserID does not pass validation. Format: '{UserIdRegex}'")
    return True


def validate_json(instance, schema):
    Validator(schema, format_checker=FormatChecker()).validate(instance)


class Schemas:
    room_message = {
        "type": "object",
        "properties": {
            "content": {
                "type": "object",
                "properties": {"msgtype": {"type": "string"}},
                "required": ["msgtype"],
            }
        },
        "not": {"required": ["state_key"]}
    }

    room_message_text = {
        "type": "object",
        "properties": {
            "content": {
                "type": "object",
                "properties": {
                    "msgtype": {"type": "string", "const": "m.text"},
                    "body": {"type": "string"},
                    "formatted_body": {"type": "string"},
                    "format": {"type": "string"},
                },
                "required": ["msgtype", "body"],
            }
        },
    }

    room_message_emote = {
        "type": "object",
        "properties": {
            "content": {
                "type": "object",
                "properties": {
                    "msgtype": {"type": "string", "const": "m.emote"},
                    "body": {"type": "string"},
                    "formatted_body": {"type": "string"},
                    "format": {"type": "string"},
                },
                "required": ["msgtype", "body"],
            }
        },
    }

    room_message_notice = {
        "type": "object",
        "properties": {
            "content": {
                "type": "object",
                "properties": {
                    "msgtype": {"type": "string", "const": "m.notice"},
                    "body": {"type": "string"},
                    "formatted_body": {"type": "string"},
                    "format": {"type": "string"},
                },
                "required": ["msgtype", "body"],
            },
        },
    }

    room_message_media = {
        "type": "object",
        "properties": {
            "content": {
                "type": "object",
                "properties": {
                    "body": {"type": "string"},
                    "url": {"type": "string"},
                    "msgtype": {
                        "type": "string",
                        "enum": ["m.image", "m.audio", "m.video", "m.file"],
                    },
                },
                "required": ["body", "url", "msgtype"],
            }
        },
    }

    room_encrypted_media = {
        "type": "object",
        "properties": {
            "content": {
                "type": "object",
                "properties": {
                    "body": {"type": "string"},
                    "msgtype": {
                        "type": "string",
                        "enum": ["m.image", "m.audio", "m.video", "m.file"],
                    },
                    "file": {
                        "type": "object",
                        "properties": {
                            "url": {"type": "string"},
                            "hashes": {
                                "type": "object",
                                "properties": {
                                    "sha256": {"type": "string"}
                                }
                            },
                            "iv": {"type": "string"},
                            "key": {
                                "type": "object",
                                "properties": {
                                    "alg": {"type": "string"},
                                    "k": {"type": "string"},
                                },
                                "required": ["alg", "k"],
                            },
                        },
                        "required": ["url", "hashes", "iv", "key"],
                    },
                    "info": {
                        "type": "object",
                        "properties": {
                            "thumbnail_file": {
                                "type": "object",
                                "properties": {
                                    "url": {"type": "string"},
                                    "hashes": {
                                        "type": "object",
                                        "properties": {
                                            "sha256": {"type": "string"}
                                        }
                                    },
                                    "iv": {"type": "string"},
                                    "key": {
                                        "type": "object",
                                        "properties": {
                                            "alg": {"type": "string"},
                                            "k": {"type": "string"},
                                        },
                                        "required": ["alg", "k"],
                                    },
                                },
                                "required": ["url", "hashes", "iv", "key"],
                            },
                        },
                    },
                },
                "required": ["body", "file", "msgtype"],
            }
        },
        "required": ["content"],
    }

    redacted_event = {
        "type": "object",
        "properties": {
            "unsigned": {
                "type": "object",
                "properties": {
                    "redacted_because": {
                        "type": "object",
                        "properties": {
                            "sender": {"type": "string", "format": "user_id"},
                            "content": {
                                "type": "object",
                                "properties": {"reason": {"type": "string"}},
                            },
                        },
                        "required": ["sender", "content"],
                    }
                },
                "required": ["redacted_because"],
            }
        },
        "required": ["unsigned"],
    }

    register = {
        "type": "object",
        "properties": {
            "user_id": {"type": "string", "format": "user_id"},
            "device_id": {"type": "string"},
            "access_token": {"type": "string"},
        },
        "required": ["user_id", "device_id", "access_token"],
    }

    login = {
        "type": "object",
        "properties": {
            "user_id": {"type": "string", "format": "user_id"},
            "device_id": {"type": "string"},
            "access_token": {"type": "string"},
        },
        "required": ["user_id", "device_id", "access_token"],
    }

    login_info = {
        "type": "object",
        "properties": {
            "flows": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "type": {"type": "string"}
                    },
                    "required": ["type"]
                }
            },
        },
        "required": ["flows"],
    }

    error = {
        "type": "object",
        "properties": {
            "error": {"type": "string"},
            "errcode": {"type": "string"},
            "retry_after_ms": {"type": "integer"},
        },
        "required": ["error", "errcode"],
    }

    room_timeline = {
        "type": "object",
        "properties": {
            "events": {"type": "array"},
            "limited": {"type": "boolean"},
            "prev_batch": {"type": "string"},
        },
        "required": ["events", "limited", "prev_batch"],
    }

    sync = {
        "type": "object",
        "properties": {
            "device_one_time_keys_count": {
                "type": "object",
                "properties": {
                    "curve25519": {"type": "integer", "default": 0},
                    "signed_curve25519": {"type": "integer", "default": 0},
                }
            },
            "device_lists": {
                "type": "object",
                "properties": {
                    "changed": {"type": "array", "items": {"type": "string"}},
                    "left": {"type": "array", "items": {"type": "string"}}
                }
            },
            "next_batch": {"type": "string"},
            "rooms": {
                "type": "object",
                "properties": {
                    "invite": {
                        "type": "object",
                        "patternProperties": {
                            RoomRegex: {
                                "type": "object",
                                "properties": {
                                    "invite_state": {
                                        "type": "object",
                                        "properties": {
                                            "events": {"type": "array"}
                                        },
                                        "required": ["events"]
                                    }
                                },
                                "required": ["invite_state"]
                            }
                        },
                        "additionalProperties": False,
                    },
                    "join": {
                        "type": "object",
                        "patternProperties": {
                            RoomRegex: {
                                "type": "object",
                                "properties": {
                                    "timeline": room_timeline,
                                    "state": {
                                        "type": "object",
                                        "properties": {
                                            "events": {"type": "array"}
                                        },
                                        "required": ["events"]
                                    },
                                    "ephemeral": {
                                        "type": "object",
                                        "properties": {
                                            "events": {"type": "array"}
                                        },
                                        "required": ["events"]
                                    },
                                    "summary": {
                                        "type": "object",
                                        "properties": {
                                            "m.invited_member_count": {
                                                "type": "integer"
                                            },
                                            "m.joined_member_count": {
                                                "type": "integer"
                                            },
                                            "m.heroes": {
                                                "type": "array",
                                                "items": {"type": "string"}
                                            },
                                        }
                                    },
                                    "account_data": {
                                        "type": "object",
                                        "properties": {
                                            "events": {"type": "array"}
                                        },
                                        "required": ["events"]
                                    },
                                },
                                "required": [
                                    "timeline",
                                    "state",
                                    "ephemeral",
                                    "account_data",
                                ]
                            }
                        },
                        "additionalProperties": False,
                    },
                    "leave": {
                        "type": "object",
                        "patternProperties": {
                            RoomRegex: {
                                "type": "object",
                                "properties": {
                                    "timeline": {
                                        "type": "object",
                                        "properties": {
                                            "events": {"type": "array"}
                                        },
                                        "required": ["events"]
                                    },
                                    "state": {
                                        "type": "object",
                                        "properties": {
                                            "events": {"type": "array"}
                                        },
                                        "required": ["events"]
                                    }
                                },
                                "required": ["timeline", "state"]
                            }
                        },
                        "additionalProperties": False,
                    },
                },
            },
            "to_device": {
                "type": "object",
                "properties": {
                    "events": {"type": "array"}
                },
                "required": ["events"]
            },
            "presence": {
                "type": "object",
                "properties": {
                    "events": {"type": "array"}
                },
                "required": ["events"]
            }
        },
        "required": [
            "next_batch",
            "device_one_time_keys_count",
            "device_lists",
            "rooms",
            "to_device",
        ],
    }

    room_event = {
        "type": "object",
        "properties": {
            "event_id": {"type": "string"},
            "sender": {"type": "string", "format": "user_id"},
            "type": {"type": "string"},
            "origin_server_ts": {"type": "integer", "minimum": 0},
            "unsigned": {
                "type": "object",
                "properties": {
                    "transaction_id": {"type": "string"},
                }
            },
        },
        "required": ["event_id", "sender", "type", "origin_server_ts"],
    }

    state_event = {
        "type": "object",
        "properties": {
            "event_id": {"type": "string"},
            "sender": {"type": "string", "format": "user_id"},
            "type": {"type": "string"},
            "state_key": {"type": "string"},
            "origin_server_ts": {"type": "integer", "minimum": 0},
            "unsigned": {
                "type": "object",
                "properties": {
                    "transaction_id": {"type": "string"},
                }
            },
        },
        "required": [
            "event_id",
            "sender",
            "type",
            "state_key",
            "origin_server_ts"
        ],
    }

    room_state = {
        "type": "array",
        "items": state_event,
    }

    sync_room_state = {
        "type": "object",
        "properties": {"events": {"type": "array"}},
        "required": ["events"],
    }

    to_device = {
        "type": "object",
        "properties": {
            "sender": {"type": "string", "format": "user_id"},
            "type": {"type": "string"},
            "content": {"type": "object"},
        },
        "required": ["sender", "type", "content"],
    }

    room_encrypted = {
        "type": "object",
        "properties": {
            "content": {
                "type": "object",
                "properties": {
                    "sender_key": {"type": "string"},
                    "algorithm": {"type": "string"}
                },
                "required": ["sender_key", "algorithm"],
            }
        },
        "required": ["content"],
    }

    room_olm_encrypted = {
        "type": "object",
        "properties": {
            "type": {"type": "string", "enum": ["m.room.encrypted"]},
            "content": {
                "type": "object",
                "properties": {
                    "sender_key": {"type": "string"},
                    "algorithm": {
                        "type": "string",
                        "enum": ["m.olm.v1.curve25519-aes-sha2"]
                    },
                    "ciphertext": {
                        "type": "object",
                        "patternProperties": {
                            Base64Regex: {
                                "type": "object",
                                "properties": {
                                    "body": {"type": "string"},
                                    "type": {"type": "integer"},
                                },
                                "required": ["type", "body"]
                            }
                        },
                    }
                },
                "required": ["sender_key", "algorithm", "ciphertext"]
            },
        },
        "required": [
            "type",
            "content",
        ],
    }

    room_megolm_decrypted = {
        "type": "object",
        "properties": {
            "type": {"type": "string"},
            "content": {"type": "object"}
        },
        "required": [
            "type",
            "content",
        ],
    }

    room_megolm_encrypted = {
        "type": "object",
        "properties": {
            "type": {"type": "string", "enum": ["m.room.encrypted"]},
            "event_id": {"type": "string"},
            "sender": {"type": "string", "format": "user_id"},
            "origin_server_ts": {"type": "integer", "minimum": 0},
            "room_id": {"type": "string"},
            "content": {
                "type": "object",
                "properties": {
                    "sender_key": {"type": "string"},
                    "algorithm": {
                        "type": "string",
                        "enum": ["m.megolm.v1.aes-sha2"]
                    },
                    "ciphertext": {"type": "string"},
                    "session_id": {"type": "string"},
                    "device_id": {"type": "string"},
                },
                "required": [
                    "sender_key",
                    "algorithm",
                    "ciphertext",
                    "session_id",
                    "device_id",
                ]
            },
        },
        "required": [
            "type",
            "content",
            "event_id",
            "sender",
            "origin_server_ts"
        ],
    }

    olm_event = {
        "type": "object",
        "properties": {
            "sender": {"type": "string", "format": "user_id"},
            "sender_device": {"type": "string"},
            "keys": {
                "type": "object",
                "properties": {"ed25519": {"type": "string"}},
            },
            "recipient": {"type": "string", "format": "user_id"},
            "recipient_keys": {
                "type": "object",
                "properties": {"ed25519": {"type": "string"}},
            },
            "type": {"type": "string"},
            "content": {"type": "object"},
        },
        "required": [
            "type",
            "sender",
            "keys",
            "recipient",
            "recipient_keys",
            "content",
        ],
    }

    dummy_event = {
        "type": "object",
        "properties": {
            "sender": {"type": "string", "format": "user_id"},
            "sender_device": {"type": "string"},
            "type": {"type": "string", "enum": ["m.dummy"]},
            "content": {
                "type": "object",
            },
            "keys": {"type": "object"},
        },
        "required": ["type", "sender", "keys", "sender_device"],
    }

    room_key_request = {
        "type": "object",
        "properties": {
            "sender": {"type": "string", "format": "user_id"},
            "type": {"type": "string", "enum": ["m.room_key_request"]},
            "content": {
                "type": "object",
                "properties": {
                    "body": {
                        "type": "object",
                        "properties": {
                            "algorithm": {"type": "string"},
                            "room_id": {"type": "string", "format": "room_id"},
                            "sender_key": {"type": "string"},
                            "session_id": {"type": "string"},
                        },
                        "required": [
                            "algorithm",
                            "room_id",
                            "sender_key",
                            "session_id"
                        ]
                    },
                    "requesting_device_id": {"type": "string"},
                    "action": {
                        "type": "string",
                        "enum": ["request", "request_cancellation"]
                    },
                    "request_id": {"type": "string"}
                },
                "required": [
                    "requesting_device_id",
                    "request_id",
                    "action",
                    "body"
                ],
            },
        },
        "required": ["type", "sender", "content"],
    }

    room_key_request_cancel = {
        "type": "object",
        "properties": {
            "sender": {"type": "string", "format": "user_id"},
            "type": {"type": "string", "enum": ["m.room_key_request"]},
            "content": {
                "type": "object",
                "properties": {
                    "requesting_device_id": {"type": "string"},
                    "action": {
                        "type": "string",
                        "enum": ["request", "request_cancellation"]
                    },
                    "request_id": {"type": "string"}
                },
                "required": [
                    "requesting_device_id",
                    "request_id",
                    "action"
                ],
            },
        },
        "required": ["type", "sender", "content"],
    }

    room_key_event = {
        "type": "object",
        "properties": {
            "sender": {"type": "string", "format": "user_id"},
            "sender_device": {"type": "string"},
            "type": {"type": "string", "enum": ["m.room_key"]},
            "content": {
                "type": "object",
                "properties": {
                    "algorithm": {"type": "string"},
                    "room_id": {"type": "string", "format": "room_id"},
                    "session_id": {"type": "string"},
                    "session_key": {"type": "string"},
                    "chain_index": {"type": "integer"},
                },
                "required": [
                    "algorithm",
                    "room_id",
                    "session_id",
                    "session_key",
                ],
            },
            "keys": {"type": "object"},
        },
        "required": ["type", "sender", "content", "keys"],
    }

    forwarded_room_key_event = {
        "type": "object",
        "properties": {
            "sender": {"type": "string", "format": "user_id"},
            "sender_device": {"type": "string"},
            "type": {"type": "string", "enum": ["m.forwarded_room_key"]},
            "content": {
                "type": "object",
                "properties": {
                    "algorithm": {"type": "string"},
                    "room_id": {"type": "string", "format": "room_id"},
                    "sender_key": {"type": "string"},
                    "sender_claimed_ed25519_key": {"type": "string"},
                    "forwarding_curve25519_key_chain": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "session_id": {"type": "string"},
                    "session_key": {"type": "string"},
                },
                "required": [
                    "algorithm",
                    "room_id",
                    "session_id",
                    "session_key",
                    "sender_key",
                    "sender_claimed_ed25519_key",
                    "forwarding_curve25519_key_chain",
                ],
            },
        },
        "required": ["type", "sender", "content"],
    }

    room_create = {
        "type": "object",
        "properties": {
            "sender": {"type": "string", "format": "user_id"},
            "state_key": {"type": "string"},
            "type": {"type": "string"},
            "content": {
                "type": "object",
                "properties": {
                    "creator": {"type": "string", "format": "user_id"},
                    "m.federate": {"type": "boolean", "default": True},
                    "room_version": {"type": "string", "default": "1"},
                },
                "required": ["creator"],
            },
        },
        "required": ["type", "sender", "content", "state_key"],
    }

    room_guest_access = {
        "type": "object",
        "properties": {
            "sender": {"type": "string", "format": "user_id"},
            "state_key": {"type": "string"},
            "type": {"type": "string"},
            "content": {
                "type": "object",
                "properties": {
                    "guest_access": {
                        "type": "string",
                        "enum": ["can_join", "forbidden"],
                        "default": "forbidden"
                    },
                },
                "required": ["guest_access"],
            },
        },
        "required": ["type", "sender", "content", "state_key"],
    }

    room_join_rules = {
        "type": "object",
        "properties": {
            "sender": {"type": "string", "format": "user_id"},
            "state_key": {"type": "string"},
            "type": {"type": "string"},
            "content": {
                "type": "object",
                "properties": {
                    "join_rule": {
                        "type": "string",
                        "enum": ["public", "knock", "invite", "private"],
                        "default": "invite",
                    },
                },
                "required": ["join_rule"],
            },
        },
        "required": ["type", "sender", "content", "state_key"],
    }

    room_history_visibility = {
        "type": "object",
        "properties": {
            "sender": {"type": "string", "format": "user_id"},
            "state_key": {"type": "string"},
            "type": {"type": "string"},
            "content": {
                "type": "object",
                "properties": {
                    "history_visibility": {
                        "type": "string",
                        "enum": [
                            "invited",
                            "joined",
                            "shared",
                            "world_readable",
                        ],
                        "default": "shared",
                    },
                },
                "required": ["history_visibility"],
            },
        },
        "required": ["type", "sender", "content", "state_key"],
    }

    room_canonical_alias = {
        "type": "object",
        "properties": {
            "sender": {"type": "string", "format": "user_id"},
            "state_key": {"type": "string"},
            "type": {"type": "string"},
            "content": {
                "type": "object",
                "properties": {"alias": {"type": "string"}},
                "required": [],
            },
        },
        "required": ["type", "sender", "content", "state_key"],
    }

    room_name = {
        "type": "object",
        "properties": {
            "sender": {"type": "string", "format": "user_id"},
            "state_key": {"type": "string"},
            "type": {"type": "string"},
            "content": {
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "required": ["name"],
            },
        },
        "required": ["type", "sender", "content", "state_key"],
    }

    room_encryption = {
        "type": "object",
        "properties": {
            "sender": {"type": "string", "format": "user_id"},
            "state_key": {"type": "string"},
            "type": {"type": "string"},
            "content": {
                "type": "object",
            },
        },
        "required": ["type", "sender", "content", "state_key"],
    }

    room_topic = {
        "type": "object",
        "properties": {
            "sender": {"type": "string", "format": "user_id"},
            "state_key": {"type": "string"},
            "type": {"type": "string"},
            "content": {
                "type": "object",
                "properties": {"topic": {"type": "string"}},
                "required": ["topic"],
            },
        },
        "required": ["type", "sender", "content", "state_key"],
    }

    room_avatar = {
        "type": "object",
        "properties": {
            "sender": {"type": "string", "format": "user_id"},
            "state_key": {"type": "string"},
            "type": {"type": "string"},
            "content": {
                "type": "object",
                "properties": {
                    "info": {
                        "h": {"type": "integer"},
                        "w": {"type": "integer"},
                        "mimetype": {"type", "string"},
                        "size": {"type": "integer"},
                    },
                    "url": {"type": "string"},
                },
                "required": ["url"]
            },
        },
        "required": ["type", "sender", "content", "state_key"]
    }

    room_power_levels = {
        "type": "object",
        "properties": {
            "sender": {"type": "string", "format": "user_id"},
            "state_key": {"type": "string"},
            "type": {"type": "string"},
            "content": {
                "type": "object",
                "properties": {
                    "ban": {"type": "integer", "default": 50},
                    "kick": {"type": "integer", "default": 50},
                    "invite": {"type": "integer", "default": 50},
                    "redact": {"type": "integer", "default": 50},
                    "users_default": {"type": "integer", "default": 0},
                    "events_default": {"type": "integer", "default": 0},
                    "state_default": {"type": "integer", "default": 50},
                    "events": {
                        "type": "object",
                        "default": {},
                        "patternProperties": {
                            EventTypeRegex: {"type": "integer"}
                        },
                        "additionalProperties": False,
                    },
                    "users": {
                        "type": "object",
                        "default": {},
                        "patternProperties": {
                            UserIdRegex: {"type": "integer"}
                        },
                        "additionalProperties": False,
                    },
                },
            },
        },
        "required": ["type", "sender", "content", "state_key"],
    }

    room_membership = {
        "type": "object",
        "properties": {
            "sender": {"type": "string", "format": "user_id"},
            "state_key": {"type": "string", "format": "user_id"},
            "type": {"type": "string", "enum": ["m.room.member"]},
            "prev_content": {
                "type": "object",
                "properties": {
                    "membership": {
                        "type": "string",
                        "enum": ["invite", "join", "knock", "leave", "ban"],
                    },
                    "avatar_url": {"type": ["string", "null"]},
                    "displayname": {"type": ["string", "null"]},
                },
                "required": ["membership"],
            },
            "content": {
                "type": "object",
                "properties": {
                    "membership": {
                        "type": "string",
                        "enum": ["invite", "join", "knock", "leave", "ban"],
                    },
                    "reason": {"type": ["string", "null"]},
                    "avatar_url": {"type": ["string", "null"]},
                    "displayname": {"type": ["string", "null"]},
                },
                "required": ["membership"],
            },
        },
        "required": ["type", "sender", "state_key", "content"],
    }

    room_redaction = {
        "type": "object",
        "properties": {
            "sender": {"type": "string", "format": "user_id"},
            "redacts": {"type": "string"},
            "content": {
                "type": "object",
                "properties": {"reason": {"type": "string"}},
            },
        },
        "required": ["sender", "redacts"],
    }

    room_resolve_alias = {
        "type": "object",
        "properties": {
            "room_id": {"type": "string"},
            "servers": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["room_id", "servers"],
    }


    room_event_id = {
        "type": "object",
        "properties": {"event_id": {"type": "string"}},
        "required": ["event_id"],
        "additionalProperties": False,
    }

    room_id = {
        "type": "object",
        "properties": {"room_id": {"type": "string"}},
        "required": ["room_id"],
        "additionalProperties": False,
    }

    room_create_response = {
        "type": "object",
        "properties": {"room_id": {"type": "string"}},
        "required": ["room_id"],
    }

    room_messages = {
        "type": "object",
        "properties": {
            "chunk": {"type": "array"},
            "start": {"type": "string"},
            "end": {"type": "string"},
        },
        "required": ["chunk", "start", "end"],
        "additionalProperties": False,
    }

    room_context = {
        "type": "object",
        "properties": {
            "start": {"type": "string"},
            "end": {"type": "string"},
            "state": {"type": "array"},
            "events_before": {"type": "array"},
            "events_after": {"type": "array"},
            "event": {"type": "object"},
        },
        "required": [
            "start",
            "end",
            "state",
            "events_before",
            "events_after",
            "event",
        ],
        "additionalProperties": False,
    }

    invite_event = {
        "type": "object",
        "properties": {
            "content": {"type": "object"},
            "type": {"type": "string"}
        },
        "required": ["content", "type"],
    }

    ephemeral_event = {
        "type": "object",
        "properties": {
            "content": {"type": "object"},
            "type": {"type": "string"}
        },
        "required": ["content", "type"],
    }

    m_typing = {
        "type": "object",
        "properties": {
            "content": {
                "type": "object",
                "properties": {
                    "user_ids": {
                        "type": "array",
                        "items": {"type": "string"}
                    }
                },
                "required": ["user_ids"]
            },
            "type": {"type": "string"},
            "room_id": {"type": "string"}
        },
        "required": ["content", "type"],
        "additionalProperties": False,
    }

    m_receipt = {
        "type": "object",
        "properties": {
            "content": {
                "type": "object",
                "patternProperties": {
                    r".*": {
                        "type": "object",
                        "properties": {
                            "m.read": {
                                "type": "object",
                                "patternProperties": {
                                    UserIdRegex: {
                                        "type": ["object", "string"],
                                        "properties": {
                                            "ts": {"type" : "integer"}
                                        },
                                        "required": ["ts"]
                                    }
                                }
                            }
                        }
                    }
                }
            },
            "type": {"type": "string"},
            "room_id": {"type": "string"}
        },
        "required": ["content", "type"],
        "additionalProperties": True,
    }

    keys_upload = {
        "type": "object",
        "properties": {
            "one_time_key_counts": {
                "type": "object",
                "properties": {
                    "curve25519": {"type": "integer", "default": 0},
                    "signed_curve25519": {"type": "integer", "default": 0},
                },
            },
        },
        "required": ["one_time_key_counts"],
    }

    keys_query = {
        "type": "object",
        "properties": {
            "device_keys": {
                "type": "object",
                "patternProperties": {UserIdRegex: {
                    "type": "object",
                    "patternProperties": {r".+": {
                        "type": "object",
                        "properties": {
                            "algorithms": {
                                "type": "array",
                                "items": {"type": "string"}
                            },
                            "device_id": {"type": "string"},
                            "user_id": {"type": "string"},
                            "keys": {
                                "type": "object",
                                "patternProperties": {
                                    KeyRegex: {"type": "string"}
                                },
                            },
                            "signatures": {
                                "type": "object",
                                "patternProperties": {
                                    UserIdRegex: {
                                        "type": "object",
                                        "patternProperties": {
                                            KeyRegex: {"type": "string"}
                                        }
                                    }
                                },
                            },
                        },
                        "required": [
                            "algorithms",
                            "device_id",
                            "keys",
                            "signatures"
                        ]
                    }}
                }},
            },
            "failures": {
                "type": "object"
            }
        },
        "required": ["device_keys", "failures"],
    }

    keys_claim = {
        "type": "object",
        "properties": {
            "one_time_keys": {
                "type": "object",
                "patternProperties": {UserIdRegex: {
                    "type": "object",
                    "patternProperties": {r".+": {
                        "type": "object",
                        "properties": {
                            "patternProperties": {SignedCurveRegex: {
                                "type": "object",
                                "properties": {
                                    "key": {"type": "str"},
                                    "signatures": {
                                        "type": "object",
                                        "patternProperties": {UserIdRegex: {
                                            "type": "object",
                                            "patternProperties": {KeyRegex: {
                                                "type": "string"
                                            }}
                                        }},
                                    },
                                },
                                "required": [
                                    "key",
                                    "signatures"
                                ]
                            }},
                        }
                    }}
                }}
            },
            "failures": {
                "type": "object"
            }
        },
        "required": ["one_time_keys", "failures"],
    }

    devices = {
        "type": "object",
        "properties": {
            "devices": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "device_id": {"type": "string"},
                        "display_name": {"type": ["string", "null"]},
                        "last_seen_ip": {"type": ["string", "null"]},
                        "last_seen_ts": {"type": ["integer", "null"]},
                    },
                    "required": [
                        "device_id",
                        "display_name",
                        "last_seen_ip",
                        "last_seen_ts"
                    ]
                }
            },
        },
        "required": ["devices"]
    }

    delete_devices = {
        "type": "object",
        "properties": {
            "session": {"type": "string"},
            "flows": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "stages": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "required": ["stages"]
                    },
                }
            },
            "params": {
                "type": "object",
                "patternProperties": {r".+": {
                    "type": "object",
                    "patternProperties": {r".+": {"type": "string"}}
                }},
            },
            "required": ["session", "flows", "params"]
        },
    }

    joined_members = {
        "type": "object",
        "properties": {
            "joined": {
                "type": "object",
                "patternProperties": {UserIdRegex: {
                    "type": "object",
                    "properties": {
                        "avatar_url": {"type": ["string", "null"]},
                        "display_name": {"type": ["string", "null"]},
                    },
                    "required": ["display_name"]
                }}
            }
        },
        "required": ["joined"]
    }

    joined_rooms = {
        "type": "object",
        "properties": {
            "joined_rooms": {
                "type": "array",
                "items": {"type": "string"}
            }
        },
        "required": ["joined_rooms"]
    }

    call_invite = {
        "type": "object",
        "properties": {
            "type": {"type": "string"},
            "content": {
                "type": "object",
                "properties": {
                    "call_id": {"type": "string"},
                    "lifetime": {"type": "integer"},
                    "version": {"type": "integer"},
                    "offer": {
                        "type": "object",
                        "properties": {
                            "type": {"type": "string", "enum": ["offer"]},
                            "sdp": {"type": "string"},
                        },
                        "required": ["type", "sdp"]
                    },
                },
                "required": [
                    "call_id",
                    "lifetime",
                    "version",
                    "offer",
                ]
            }
        },
        "required": [
            "type",
            "content",
        ],
    }

    call_answer = {
        "type": "object",
        "properties": {
            "type": {"type": "string"},
            "content": {
                "type": "object",
                "properties": {
                    "call_id": {"type": "string"},
                    "version": {"type": "integer"},
                    "answer": {
                        "type": "object",
                        "properties": {
                            "type": {"type": "string", "enum": ["answer"]},
                            "sdp": {"type": "string"},
                        },
                        "required": ["type", "sdp"]
                    },
                },
                "required": [
                    "call_id",
                    "version",
                    "answer",
                ]
            }
        },
        "required": [
            "type",
            "content",
        ],
    }

    call_hangup = {
        "type": "object",
        "properties": {
            "type": {"type": "string"},
            "content": {
                "type": "object",
                "properties": {
                    "call_id": {"type": "string"},
                    "version": {"type": "integer"},
                },
                "required": [
                    "call_id",
                    "version",
                ]
            }
        },
        "required": [
            "type",
            "content",
        ],
    }

    call_candidates = {
        "type": "object",
        "properties": {
            "type": {"type": "string"},
            "content": {
                "type": "object",
                "properties": {
                    "call_id": {"type": "string"},
                    "version": {"type": "integer"},
                    "candidates": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "candidate": {"type": "string"},
                                "sdpMLineIndex": {"type": "integer"},
                                "sdpMid": {"type": "string"},
                            },
                            "required": [
                                "candidate",
                                "sdpMLineIndex",
                                "sdpMid"
                            ]
                        }
                    }
                },
                "required": [
                    "call_id",
                    "version",
                    "candidates"
                ]
            }
        },
        "required": [
            "type",
            "content",
        ],
    }

    account_data = {
        "type": "object",
        "properties": {
            "type": {"type": "string"},
            "content": {"type": "object"}
        },
        "required": [
            "type",
            "content",
        ],
    }

    fully_read = {
        "type": "object",
        "properties": {
            "type": {"type": "string"},
            "content": {
                "type": "object",
                "properties": {
                    "event_id": {"type": "string"},
                },
                "required": [
                    "event_id",
                ]
            }
        },
        "required": [
            "type",
            "content",
        ],
    }

    tags = {
        "type": "object",
        "properties": {
            "type": {"type": "string"},
            "content": {
                "type": "object",
                "properties": {
                    "tags": {
                        "type:": "object",
                        "patternProperties": {
                            r".*": {
                                "type": "object",
                                "properties": {
                                    "order": {"type": "number"}
                                },
                            },
                        },
                    },
                },
                "required": [
                    "tags"
                ],
            },
        },
        "required": [
            "type",
            "content",
        ],
    }

    upload = {
        "type": "object",
        "properties": {"content_uri": {"type": "string"}},
        "required": ["content_uri"],
        "additionalProperties": False,
    }

    content_repository_config = {
        "type": "object",
        "properties": {"m.upload.size": {"type": ["number", "null"]}},
    }

    megolm_key_import = {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "algorithm": {"type": "string"},
                "session_key": {"type": "string"},
                "sender_key": {"type": "string"},
                "room_id": {"type": "string"},
                "sender_claimed_keys": {
                    "type": "object",
                    "properties": {
                        "ed25519": {"type": "string"},
                    },
                    "required": ["ed25519"]
                },
                "forwarding_curve25519_key_chain": {
                    "type": "array",
                    "items": {"type": "string"}
                },
            },
            "required": [
                "algorithm",
                "session_key",
                "sender_key",
                "room_id",
                "sender_claimed_keys",
                "forwarding_curve25519_key_chain"
            ]
        }
    }

    get_profile = {
        "type": "object",
        "properties": {
            "displayname": {"type": "string"},
            "avatar_url": {"type": "string"},
        },
        "not": {"required": ["errcode"]}
    }

    get_displayname = {
        "type": "object",
        "properties": {
            "displayname": {"type": ["string", "null"]},
        },
        "required": ["displayname"]
    }

    get_avatar = {
        "type": "object",
        "properties": {
            "avatar_url": {"type": ["string", "null"]},
        },
        "required": ["avatar_url"]
    }

    key_verification_start = {
        "type": "object",
        "properties": {
            "sender": {"type": "string"},
            "content": {
                "type": "object",
                "properties": {
                    "transaction_id": {"type": "string"},
                    "from_device": {"type": "string"},
                    "method": {"type": "string"},
                    "key_agreement_protocols": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "hashes": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "message_authentication_codes": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "short_authentication_string": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                },
                "required": [
                    "transaction_id",
                    "from_device",
                    "method",
                    "key_agreement_protocols",
                    "hashes",
                    "message_authentication_codes",
                    "short_authentication_string",
                ]
            }
        },
        "required": [
            "sender",
            "content",
        ],
    }

    key_verification_accept = {
        "type": "object",
        "properties": {
            "sender": {"type": "string"},
            "content": {
                "type": "object",
                "properties": {
                    "transaction_id": {"type": "string"},
                    "commitment": {"type": "string"},
                    "key_agreement_protocol": {"type": "string"},
                    "hash": {"type": "string"},
                    "message_authentication_code": {"type": "string"},
                    "short_authentication_string": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                },
                "required": [
                    "transaction_id",
                    "commitment",
                    "key_agreement_protocol",
                    "hash",
                    "message_authentication_code",
                    "short_authentication_string",
                ]
            }
        },
        "required": [
            "sender",
            "content",
        ],
    }

    key_verification_key = {
        "type": "object",
        "properties": {
            "sender": {"type": "string"},
            "content": {
                "type": "object",
                "properties": {
                    "transaction_id": {"type": "string"},
                    "key": {"type": "string"},
                },
                "required": [
                    "transaction_id",
                    "key",
                ]
            }
        },
        "required": [
            "sender",
            "content",
        ],
    }

    key_verification_mac = {
        "type": "object",
        "properties": {
            "sender": {"type": "string"},
            "content": {
                "type": "object",
                "properties": {
                    "mac": {
                        "type": "object",
                        "patternProperties": {
                            r".+": {"type": "string"}
                        }
                    },
                    "keys": {"type": "string"},
                },
                "required": [
                    "transaction_id",
                    "mac",
                    "keys",
                ]
            }
        },
        "required": [
            "sender",
            "content",
        ],
    }

    key_verification_cancel = {
        "type": "object",
        "properties": {
            "sender": {"type": "string"},
            "content": {
                "type": "object",
                "properties": {
                    "transaction_id": {"type": "string"},
                    "code": {"type": "string"},
                    "reason": {"type": "string"},
                },
                "required": [
                    "transaction_id",
                    "code",
                    "reason",
                ]
            }
        },
        "required": [
            "sender",
            "content"
        ],
    }

    presence = {
        "type": "object",
        "properties": {
            "sender": {"type": "string"},
            "type": {"type": "string", "const": "m.presence"},
            "content": {
                "type": "object",
                "properties": {
                    "presence": {"type": "string"},
                    "currently_active": {"type": "boolean"},
                    "last_active_ago": {"type": "integer"},
                    "status_msg": {"type": "string"},
                },
                "required": [
                    "presence",
                ]
            }
        },
        "required": [
            "sender",
            "type",
            "content"
        ],
    }

    get_presence = {
        "type": "object",
        "properties": {
            "presence": {"type": "string"},
            "last_active_ago": {"type": "integer"},
            "status_msg": {"type": "string"},
            "currently_active": {"type": "boolean"},
        },
        "required": [
            "presence",
        ],
    }

    empty = {"type": "object", "properties": {}, "additionalProperties": False}
