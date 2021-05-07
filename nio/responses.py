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

from builtins import str
from dataclasses import dataclass, field
from datetime import datetime
from functools import wraps
from typing import Any, Dict, Generator, List, Optional, Set, Tuple, Union

from jsonschema.exceptions import SchemaError, ValidationError
from logbook import Logger

from .event_builders import ToDeviceMessage
from .events import (AccountDataEvent, BadEventType, Event, InviteEvent,
                     ToDeviceEvent, EphemeralEvent)
from .events.presence import PresenceEvent
from .http import TransportResponse
from .log import logger_group
from .schemas import Schemas, validate_json

logger = Logger("nio.responses")
logger_group.add_logger(logger)


__all__ = [
    "ConnectionInfo",
    "ContentRepositoryConfigResponse",
    "ContentRepositoryConfigError",
    "FileResponse",
    "DeleteDevicesAuthResponse",
    "DeleteDevicesResponse",
    "DeleteDevicesError",
    "DeletePushRuleError",
    "DeletePushRuleResponse",
    "Device",
    "DeviceInfo",
    "DeviceList",
    "DevicesResponse",
    "DevicesError",
    "DeviceOneTimeKeyCount",
    "DiscoveryInfoError",
    "DiscoveryInfoResponse",
    "DownloadResponse",
    "DownloadError",
    "EnablePushRuleResponse",
    "EnablePushRuleError",
    "ErrorResponse",
    "InviteInfo",
    "JoinResponse",
    "JoinError",
    "JoinedMembersResponse",
    "JoinedMembersError",
    "JoinedRoomsResponse",
    "JoinedRoomsError",
    "KeysClaimResponse",
    "KeysClaimError",
    "KeysQueryResponse",
    "KeysQueryError",
    "KeysUploadResponse",
    "KeysUploadError",
    "RegisterResponse",
    "LoginResponse",
    "LoginError",
    "LoginInfoResponse",
    "LoginInfoError",
    "LogoutResponse",
    "LogoutError",
    "Response",
    "RoomBanResponse",
    "RoomBanError",
    "RoomCreateResponse",
    "RoomCreateError",
    "RoomInfo",
    "RoomInviteResponse",
    "RoomInviteError",
    "RoomKickResponse",
    "RoomKickError",
    "RoomLeaveResponse",
    "RoomLeaveError",
    "RoomForgetResponse",
    "RoomForgetError",
    "RoomMember",
    "RoomMessagesResponse",
    "RoomMessagesError",
    "RoomGetStateResponse",
    "RoomGetStateError",
    "RoomGetStateEventResponse",
    "RoomGetStateEventError",
    "RoomGetEventResponse",
    "RoomGetEventError",
    "RoomPutStateResponse",
    "RoomPutStateError",
    "RoomRedactResponse",
    "RoomRedactError",
    "RoomResolveAliasResponse",
    "RoomResolveAliasError",
    "RoomSendResponse",
    "RoomSendError",
    "RoomSummary",
    "RoomUnbanResponse",
    "RoomUnbanError",
    "Rooms",
    "SetPushRuleError",
    "SetPushRuleResponse",
    "SetPushRuleActionsError",
    "SetPushRuleActionsResponse",
    "ShareGroupSessionResponse",
    "ShareGroupSessionError",
    "SyncResponse",
    "SyncError",
    "Timeline",
    "UpdateDeviceResponse",
    "UpdateDeviceError",
    "RoomTypingResponse",
    "RoomTypingError",
    "RoomReadMarkersResponse",
    "RoomReadMarkersError",
    "SessionInfo",
    "UploadResponse",
    "UploadError",
    "ProfileGetResponse",
    "ProfileGetError",
    "ProfileGetDisplayNameResponse",
    "ProfileGetDisplayNameError",
    "ProfileSetDisplayNameResponse",
    "ProfileSetDisplayNameError",
    "ProfileGetAvatarResponse",
    "ProfileGetAvatarError",
    "ProfileSetAvatarResponse",
    "ProfileSetAvatarError",
    "PresenceGetResponse",
    "PresenceGetError",
    "PresenceSetResponse",
    "PresenceSetError",
    "RoomKeyRequestResponse",
    "RoomKeyRequestError",
    "ThumbnailResponse",
    "ThumbnailError",
    "ToDeviceResponse",
    "ToDeviceError",
    "RoomContextResponse",
    "RoomContextError",
    "UploadFilterError",
    "UploadFilterResponse",
    "UpdateReceiptMarkerError",
    "UpdateReceiptMarkerResponse",
    "WhoisResponse",
]


def verify(schema, error_class, pass_arguments=True):
    def decorator(f):
        @wraps(f)
        def wrapper(cls, parsed_dict, *args, **kwargs):
            try:
                logger.info("Validating response schema")
                validate_json(parsed_dict, schema)
            except (SchemaError, ValidationError) as e:
                logger.warn("Error validating response: " + str(e.message))

                if pass_arguments:
                    return error_class.from_dict(parsed_dict, *args, **kwargs)
                else:
                    return error_class.from_dict(parsed_dict)

            return f(cls, parsed_dict, *args, **kwargs)
        return wrapper
    return decorator


@dataclass
class Rooms:
    invite: Dict[str, "InviteInfo"] = field()
    join: Dict[str, "RoomInfo"] = field()
    leave: Dict[str, "RoomInfo"] = field()


@dataclass
class DeviceOneTimeKeyCount:
    curve25519: Optional[int] = field()
    signed_curve25519: Optional[int] = field()


@dataclass
class DeviceList:
    changed: List[str] = field()
    left: List[str] = field()


@dataclass
class Timeline:
    events: List = field()
    limited: bool = field()
    prev_batch: str = field()


@dataclass
class InviteInfo:
    invite_state: List = field()


@dataclass
class RoomSummary:
    invited_member_count: Optional[int] = None
    joined_member_count: Optional[int] = None
    heroes: Optional[List[str]] = None


@dataclass
class UnreadNotifications:
    notification_count: Optional[int] = None
    highlight_count: Optional[int] = None


@dataclass
class RoomInfo:
    timeline: Timeline = field()
    state: List = field()
    ephemeral: List = field()
    account_data: List = field()
    summary: Optional[RoomSummary] = None
    unread_notifications: Optional[UnreadNotifications] = None

    @staticmethod
    def parse_account_data(event_dict):
        """Parse the account data dictionary and produce a list of events."""
        events = []

        for event in event_dict:
            events.append(AccountDataEvent.parse_event(event))

        return events


@dataclass
class RoomMember:
    user_id: str = field()
    display_name: str = field()
    avatar_url: str = field()


@dataclass
class Device:
    id: str = field()
    display_name: str = field()
    last_seen_ip: str = field()
    last_seen_date: datetime = field()

    @classmethod
    def from_dict(cls, parsed_dict):
        date = None

        if parsed_dict["last_seen_ts"] is not None:
            date = datetime.fromtimestamp(parsed_dict["last_seen_ts"] / 1000)

        return cls(
            parsed_dict["device_id"],
            parsed_dict["display_name"],
            parsed_dict["last_seen_ip"],
            date
        )


@dataclass
class Response:
    uuid: str = field(default="", init=False)
    start_time: Optional[float] = field(default=None, init=False)
    end_time: Optional[float] = field(default=None, init=False)
    timeout: int = field(default=0, init=False)
    transport_response: Optional[TransportResponse] = field(
        init=False, default=None,
    )

    @property
    def elapsed(self):
        if not self.start_time or not self.end_time:
            return 0
        elapsed = self.end_time - self.start_time
        return max(0, elapsed - (self.timeout / 1000))


@dataclass
class FileResponse(Response):
    """A response representing a successful file content request.

    Attributes:
        body (bytes): The file's content in bytes.
        content_type (str): The content MIME type of the file,
            e.g. "image/png".
        filename (str, optional): The file's name returned by the server.
    """

    body: bytes = field()
    content_type: str = field()
    filename: Optional[str] = field()

    def __str__(self):
        return "{} bytes, content type: {}, filename: {}".format(
            len(self.body),
            self.content_type,
            self.filename
        )

    @classmethod
    def from_data(cls, data, content_type, filename=None):
        """Create a FileResponse from file content returned by the server.

        Args:
            data (bytes): The file's content in bytes.
            content_type (str): The content MIME type of the file,
                e.g. "image/png".
        """
        raise NotImplementedError()


@dataclass
class ErrorResponse(Response):
    message: str = field()
    status_code: Optional[str] = None
    retry_after_ms: Optional[int] = None
    soft_logout: bool = False

    def __str__(self) -> str:
        if self.status_code and self.message:
            e = "{} {}".format(self.status_code, self.message)
        elif self.message:
            e = self.message
        elif self.status_code:
            e = "{} unknown error".format(self.status_code)
        else:
            e = "unknown error"

        if self.retry_after_ms:
            e = "{} - retry after {}ms".format(e, self.retry_after_ms)

        return "{}: {}".format(self.__class__.__name__, e)

    @classmethod
    def from_dict(cls, parsed_dict: Dict[Any, Any]):
        # type: (...) -> ErrorResponse
        try:
            validate_json(parsed_dict, Schemas.error)
        except (SchemaError, ValidationError):
            return cls("unknown error")

        return cls(
            parsed_dict["error"],
            parsed_dict["errcode"],
            parsed_dict.get("retry_after_ms"),
            parsed_dict.get("soft_logout", False),
        )


@dataclass
class _ErrorWithRoomId(ErrorResponse):
    room_id: str = ""

    @classmethod
    def from_dict(cls, parsed_dict, room_id):
        try:
            validate_json(parsed_dict, Schemas.error)
        except (SchemaError, ValidationError):
            return cls("unknown error")

        return cls(
            parsed_dict["error"],
            parsed_dict["errcode"],
            parsed_dict.get("retry_after_ms"),
            parsed_dict.get("soft_logout", False),
            room_id
        )


class LoginError(ErrorResponse):
    pass


class LogoutError(ErrorResponse):
    pass


class SyncError(ErrorResponse):
    pass


class RoomSendError(_ErrorWithRoomId):
    pass


class RoomGetStateError(_ErrorWithRoomId):
    """A response representing an unsuccessful room state query."""
    pass


class RoomGetStateEventError(_ErrorWithRoomId):
    """A response representing an unsuccessful room state query."""
    pass


class RoomGetEventError(ErrorResponse):
    """A response representing an unsuccessful room get event request."""
    pass


class RoomPutStateError(_ErrorWithRoomId):
    """A response representing an unsuccessful room state sending request."""
    pass


class RoomRedactError(_ErrorWithRoomId):
    pass


class RoomResolveAliasError(ErrorResponse):
    """A response representing an unsuccessful room alias query."""
    pass


class RoomTypingError(_ErrorWithRoomId):
    """A response representing a unsuccessful room typing request."""

    pass


class UpdateReceiptMarkerError(ErrorResponse):
    pass


class RoomReadMarkersError(_ErrorWithRoomId):
    """A response representing a unsuccessful room read markers request."""

    pass


class RoomKickError(ErrorResponse):
    pass


class RoomBanError(ErrorResponse):
    pass


class RoomUnbanError(ErrorResponse):
    pass


class RoomInviteError(ErrorResponse):
    pass


class RoomCreateError(ErrorResponse):
    """A response representing a unsuccessful create room request."""
    pass


class JoinError(ErrorResponse):
    pass


class RoomLeaveError(ErrorResponse):
    pass


class RoomForgetError(_ErrorWithRoomId):
    pass


class RoomMessagesError(_ErrorWithRoomId):
    pass


class KeysUploadError(ErrorResponse):
    pass


class KeysQueryError(ErrorResponse):
    pass


class KeysClaimError(_ErrorWithRoomId):
    pass


class ContentRepositoryConfigError(ErrorResponse):
    """A response for a unsuccessful content repository config request."""


class UploadError(ErrorResponse):
    """A response representing a unsuccessful upload request."""


class DownloadError(ErrorResponse):
    """A response representing a unsuccessful download request."""


class ThumbnailError(ErrorResponse):
    """A response representing a unsuccessful thumbnail request."""


@dataclass
class ShareGroupSessionError(_ErrorWithRoomId):
    """Response representing unsuccessful group sessions sharing request."""

    users_shared_with: Set[Tuple[str, str]] = field(default_factory=set)

    @classmethod
    def from_dict(cls, parsed_dict, room_id, users_shared_with):
        try:
            validate_json(parsed_dict, Schemas.error)
        except (SchemaError, ValidationError):
            return cls("unknown error")

        return cls(parsed_dict["error"], parsed_dict["errcode"], room_id,
                   users_shared_with)


class DevicesError(ErrorResponse):
    pass


class DeleteDevicesError(ErrorResponse):
    pass


class UpdateDeviceError(ErrorResponse):
    pass


class JoinedMembersError(_ErrorWithRoomId):
    pass


class JoinedRoomsError(ErrorResponse):
    """A response representing an unsuccessful joined rooms query."""
    pass


class ProfileGetError(ErrorResponse):
    pass


class ProfileGetDisplayNameError(ErrorResponse):
    pass


class ProfileSetDisplayNameError(ErrorResponse):
    pass


class ProfileGetAvatarError(ErrorResponse):
    pass


class PresenceGetError(ErrorResponse):
    """Response representing a unsuccessful get presence request."""
    pass


class PresenceSetError(ErrorResponse):
    """Response representing a unsuccessful set presence request."""
    pass


class ProfileSetAvatarError(ErrorResponse):
    pass

@dataclass
class DiscoveryInfoError(ErrorResponse):
    pass


@dataclass
class DiscoveryInfoResponse(Response):
    """A response for a successful discovery info request.

    Attributes:
        homeserver_url (str): The base URL of the homeserver corresponding to
            the requested domain.

        identity_server_url (str, optional): The base URL of the identity
            server corresponding to the requested domain, if any.
    """

    homeserver_url: str = field()
    identity_server_url: Optional[str] = None

    @classmethod
    @verify(Schemas.discovery_info, DiscoveryInfoError)
    def from_dict(
        cls, parsed_dict: Dict[str, Any],
    ) -> Union["DiscoveryInfoResponse", DiscoveryInfoError]:

        homeserver_url = parsed_dict["m.homeserver"]["base_url"].rstrip("/")

        identity_server_url = parsed_dict.get(
            "m.identity_server", {},
        ).get("base_url", "").rstrip("/") or None

        return cls(homeserver_url, identity_server_url)



@dataclass
class RegisterErrorResponse(ErrorResponse):
    pass


@dataclass
class RegisterResponse(Response):
    user_id: str = field()
    device_id: str = field()
    access_token: str = field()

    def __str__(self) -> str:
        return "Registered {}, device id {}.".format(
            self.user_id, self.device_id,
        )

    @classmethod
    @verify(Schemas.register, RegisterErrorResponse)
    def from_dict(cls, parsed_dict):
        return cls(
            parsed_dict["user_id"],
            parsed_dict["device_id"],
            parsed_dict["access_token"],
        )


@dataclass
class LoginInfoError(ErrorResponse):
    pass


@dataclass
class LoginInfoResponse(Response):
    flows: List[str] = field()

    @classmethod
    @verify(Schemas.login_info, LoginInfoError)
    def from_dict(cls, parsed_dict: Dict[Any, Any]):
        # type: (...) -> Union[LoginInfoResponse, ErrorResponse]
        flow_types = [flow["type"] for flow in parsed_dict["flows"]]
        return cls(flow_types)


@dataclass
class LoginResponse(Response):
    user_id: str = field()
    device_id: str = field()
    access_token: str = field()

    def __str__(self) -> str:
        return "Logged in as {}, device id: {}.".format(
            self.user_id, self.device_id
        )

    @classmethod
    @verify(Schemas.login, LoginError)
    def from_dict(cls, parsed_dict: Dict[Any, Any]):
        # type: (...) -> Union[LoginResponse, ErrorResponse]
        return cls(
            parsed_dict["user_id"],
            parsed_dict["device_id"],
            parsed_dict["access_token"],
        )


@dataclass
class LogoutResponse(Response):
    def __str__(self) -> str:
        return "Logged out"

    @classmethod
    @verify(Schemas.empty, LogoutError)
    def from_dict(cls, parsed_dict: Dict[Any, Any]):
        # type: (...) -> Union[LogoutResponse, ErrorResponse]
        """Create a response for logout response from server."""
        return cls()


@dataclass
class JoinedMembersResponse(Response):
    members: List[RoomMember] = field()
    room_id: str = field()

    @classmethod
    @verify(Schemas.joined_members, JoinedMembersError)
    def from_dict(
        cls,
        parsed_dict: Dict[Any, Any],
        room_id: str,
    ):
        # type: (...) -> Union[JoinedMembersResponse, ErrorResponse]
        members = []

        for user_id, user_info in parsed_dict["joined"].items():
            user = RoomMember(
                user_id,
                user_info.get("display_name", None),
                user_info.get("avatar_url", None)
            )
            members.append(user)

        return cls(members, room_id)


@dataclass
class JoinedRoomsResponse(Response):
    """A response containing a list of joined rooms.

    Attributes:
        rooms (List[str]): The rooms joined by the account.
    """

    rooms: List[str] = field()

    @classmethod
    @verify(Schemas.joined_rooms, JoinedRoomsError)
    def from_dict(
        cls,
        parsed_dict: Dict[Any, Any],
    ):
        # type: (...) -> Union[JoinedRoomsResponse, ErrorResponse]
        return cls(parsed_dict["joined_rooms"])


@dataclass
class ContentRepositoryConfigResponse(Response):
    """A response for a successful content repository config request.

    Attributes:
        upload_size (Optional[int]): The maximum file size in bytes for an
            upload. If `None`, the limit is unknown.
    """

    upload_size: Optional[int] = None

    @classmethod
    @verify(Schemas.content_repository_config, ContentRepositoryConfigError)
    def from_dict(
        cls,
        parsed_dict: dict,
    ) -> Union["ContentRepositoryConfigResponse", ErrorResponse]:
        return cls(parsed_dict.get("m.upload.size"))


@dataclass
class UploadResponse(Response):
    """A response representing a successful upload request."""

    content_uri: str = field()

    @classmethod
    @verify(Schemas.upload, UploadError)
    def from_dict(cls, parsed_dict: Dict[Any, Any]):
        # type: (...) -> Union[UploadResponse, ErrorResponse]
        return cls(
            parsed_dict["content_uri"],
        )


@dataclass
class DownloadResponse(FileResponse):
    """A response representing a successful download request."""

    @classmethod
    def from_data(
            cls,
            data: bytes,
            content_type: str,
            filename: Optional[str] = None
    ):
        # type: (...) -> Union[DownloadResponse, DownloadError]
        if isinstance(data, bytes):
            return cls(body=data, content_type=content_type, filename=filename)

        if isinstance(data, dict):
            return DownloadError.from_dict(data)

        return DownloadError("invalid data")


@dataclass
class ThumbnailResponse(FileResponse):
    """A response representing a successful thumbnail request."""

    @classmethod
    def from_data(
            cls,
            data: bytes,
            content_type: str,
            filename: Optional[str] = None
    ):
        # type: (...) -> Union[ThumbnailResponse, ThumbnailError]
        if not content_type.startswith("image/"):
            return ThumbnailError(f"invalid content type: {content_type}")

        if isinstance(data, bytes):
            return cls(body=data, content_type=content_type, filename=filename)

        if isinstance(data, dict):
            return ThumbnailError.from_dict(data)

        return ThumbnailError("invalid data")


@dataclass
class RoomEventIdResponse(Response):
    event_id: str = field()
    room_id: str = field()

    @staticmethod
    def create_error(parsed_dict, _room_id):
        return ErrorResponse.from_dict(parsed_dict)

    @classmethod
    def from_dict(
        cls,
        parsed_dict: Dict[Any, Any],
        room_id: str,
    ):
        # type: (...) -> Union[RoomEventIdResponse, ErrorResponse]
        try:
            validate_json(parsed_dict, Schemas.room_event_id)
        except (SchemaError, ValidationError):
            return cls.create_error(parsed_dict, room_id)

        return cls(parsed_dict["event_id"], room_id)


class RoomSendResponse(RoomEventIdResponse):
    @staticmethod
    def create_error(parsed_dict, room_id):
        return RoomSendError.from_dict(parsed_dict, room_id)


@dataclass
class RoomGetStateResponse(Response):
    """A response containing the state of a room.

    Attributes:
        events (List): The events making up the room state.
        room_id (str): The ID of the room.
    """

    events: List = field()
    room_id: str = field()

    @staticmethod
    def create_error(parsed_dict, room_id):
        return RoomGetStateError.from_dict(parsed_dict, room_id)

    @classmethod
    def from_dict(
        cls,
        parsed_dict: List[Dict[Any, Any]],
        room_id: str,
    ):
        # type: (...) -> Union[RoomGetStateResponse, RoomGetStateError]
        try:
            validate_json(parsed_dict, Schemas.room_state)
        except (SchemaError, ValidationError):
            return cls.create_error(parsed_dict, room_id)

        return cls(parsed_dict, room_id)


@dataclass
class RoomGetStateEventResponse(Response):
    """A response containing the content of a specific bit of room state.

    Attributes:
        content (Dict): The content of the state event.
        event_type (str): The type of the state event.
        state_key (str): The key of the state event.
        room_id (str): The ID of the room that the state event comes from.
    """

    content: Dict = field()
    event_type: str = field()
    state_key: str = field()
    room_id: str = field()

    @staticmethod
    def create_error(parsed_dict, room_id):
        return RoomGetStateEventError.from_dict(parsed_dict, room_id)

    @classmethod
    def from_dict(
        cls,
        parsed_dict: Dict[str, Any],
        event_type: str,
        state_key: str,
        room_id: str,
    ) -> Union["RoomGetStateEventResponse", RoomGetStateEventError]:
        return cls(parsed_dict, event_type, state_key, room_id)


class RoomGetEventResponse(Response):
    """A response indicating successful room get event request.

    Attributes:
        event (Event): The requested event.
    """

    event: Event = field()

    @classmethod
    @verify(
        Schemas.room_event,
        RoomGetEventError,
        pass_arguments=False,
    )
    def from_dict(
        cls,
        parsed_dict: Dict[str, Any]
    ) -> Union["RoomGetEventResponse", RoomGetEventError]:
        event = Event.parse_event(parsed_dict)
        resp = cls()
        resp.event = event
        return resp


class RoomPutStateResponse(RoomEventIdResponse):
    """A response indicating successful sending of room state."""
    @staticmethod
    def create_error(parsed_dict, room_id):
        return RoomPutStateError.from_dict(parsed_dict, room_id)


class RoomRedactResponse(RoomEventIdResponse):
    @staticmethod
    def create_error(parsed_dict, room_id):
        return RoomRedactError.from_dict(parsed_dict, room_id)


@dataclass
class RoomResolveAliasResponse(Response):
    """A response containing the result of resolving an alias.

    Attributes:
        room_alias (str): The alias of the room.
        room_id (str): The resolved id of the room.
        servers (List[str]): Servers participating in the room.
    """
    room_alias: str = field()
    room_id: str = field()
    servers: List[str] = field()

    @classmethod
    @verify(
        Schemas.room_resolve_alias,
        RoomResolveAliasError,
        pass_arguments=False,
    )
    def from_dict(
        cls,
        parsed_dict: Dict[Any, Any],
        room_alias: str,
    ):
        # type: (...) -> Union[RoomResolveAliasResponse, ErrorResponse]
        room_id = parsed_dict["room_id"]
        servers = parsed_dict["servers"]
        return cls(room_alias, room_id, servers)


class EmptyResponse(Response):
    @staticmethod
    def create_error(parsed_dict):
        return ErrorResponse.from_dict(parsed_dict)

    @classmethod
    def from_dict(cls, parsed_dict: Dict[Any, Any]):
        # type: (...) -> Union[Any, ErrorResponse]
        try:
            validate_json(parsed_dict, Schemas.empty)
        except (SchemaError, ValidationError):
            return cls.create_error(parsed_dict)

        return cls()


@dataclass
class _EmptyResponseWithRoomId(Response):
    room_id: str = field()

    @staticmethod
    def create_error(parsed_dict, room_id):
        return _ErrorWithRoomId.from_dict(parsed_dict, room_id)

    @classmethod
    def from_dict(cls, parsed_dict: Dict[Any, Any], room_id: str):
        # type: (...) -> Union[Any, ErrorResponse]
        try:
            validate_json(parsed_dict, Schemas.empty)
        except (SchemaError, ValidationError):
            return cls.create_error(parsed_dict, room_id)

        return cls(room_id)


class RoomKickResponse(EmptyResponse):
    @staticmethod
    def create_error(parsed_dict):
        return RoomKickError.from_dict(parsed_dict)


class RoomBanResponse(EmptyResponse):
    @staticmethod
    def create_error(parsed_dict):
        return RoomBanError.from_dict(parsed_dict)


class RoomUnbanResponse(EmptyResponse):
    @staticmethod
    def create_error(parsed_dict):
        return RoomUnbanError.from_dict(parsed_dict)


class RoomInviteResponse(EmptyResponse):
    @staticmethod
    def create_error(parsed_dict):
        return RoomInviteError.from_dict(parsed_dict)


@dataclass
class ShareGroupSessionResponse(Response):
    """Response representing a successful group sessions sharing request.

    Attributes:
        room_id (str): The room id of the group session.
        users_shared_with (Set[Tuple[str, str]]): A set containing a tuple of
            user id device id pairs with whom we shared the group session in
            this request.

    """

    room_id: str = field()
    users_shared_with: set = field()

    @classmethod
    @verify(Schemas.empty, ShareGroupSessionError)
    def from_dict(
        cls,
        _: Dict[Any, Any],
        room_id: str,
        users_shared_with: Set[Tuple[str, str]],
    ):
        # type: (...) -> Union[ShareGroupSessionResponse, ErrorResponse]
        """Create a response from the json dict the server returns.

        Args:
           parsed_dict (Dict): The dict containing the raw json response.
           room_id (str): The room id of the room to which the group session
               belongs to.
           users_shared_with (Set[Tuple[str, str]]): A set containing a tuple
               of user id device id pairs with whom we shared the group
               session in this request.
        """
        return cls(room_id, users_shared_with)


class RoomTypingResponse(_EmptyResponseWithRoomId):
    """A response representing a successful room typing request."""

    @staticmethod
    def create_error(parsed_dict, room_id):
        return RoomTypingError.from_dict(parsed_dict, room_id)


class UpdateReceiptMarkerResponse(EmptyResponse):
    @staticmethod
    def create_error(parsed_dict):
        return UpdateReceiptMarkerError.from_dict(parsed_dict)


class RoomReadMarkersResponse(_EmptyResponseWithRoomId):
    """A response representing a successful room read markers request."""

    @staticmethod
    def create_error(parsed_dict, room_id):
        return RoomTypingError.from_dict(parsed_dict, room_id)


@dataclass
class DeleteDevicesAuthResponse(Response):
    session: str = field()
    flows: Dict = field()
    params: Dict = field()

    @classmethod
    @verify(Schemas.delete_devices, DeleteDevicesError)
    def from_dict(
        cls,
        parsed_dict: Dict[Any, Any],
    ):
        # type: (...) -> Union[DeleteDevicesAuthResponse, ErrorResponse]
        return cls(
            parsed_dict["session"],
            parsed_dict["flows"],
            parsed_dict["params"]
        )


class DeleteDevicesResponse(EmptyResponse):
    @staticmethod
    def create_error(parsed_dict):
        return DeleteDevicesError.from_dict(parsed_dict)


@dataclass
class RoomMessagesResponse(Response):
    room_id: str = field()

    chunk: List[Union[Event, BadEventType]] = field()
    start: str = field()
    end: str = field()

    @classmethod
    @verify(Schemas.room_messages, RoomMessagesError)
    def from_dict(
        cls,
        parsed_dict: Dict[Any, Any],
        room_id: str,
    ):
        # type: (...) -> Union[RoomMessagesResponse, ErrorResponse]
        chunk: List[Union[Event, BadEventType]] = []
        chunk = SyncResponse._get_room_events(parsed_dict["chunk"])
        return cls(room_id, chunk, parsed_dict["start"], parsed_dict["end"])


@dataclass
class RoomIdResponse(Response):
    room_id: str = field()

    @staticmethod
    def create_error(parsed_dict):
        return ErrorResponse.from_dict(parsed_dict)

    @classmethod
    def from_dict(cls, parsed_dict: Dict[Any, Any]):
        # type: (...) -> Union[RoomIdResponse, ErrorResponse]
        try:
            validate_json(parsed_dict, Schemas.room_id)
        except (SchemaError, ValidationError):
            return cls.create_error(parsed_dict)

        return cls(parsed_dict["room_id"])

@dataclass
class RoomCreateResponse(Response):
    """Response representing a successful create room request."""
    room_id: str = field()


    @classmethod
    @verify(
        Schemas.room_create_response, RoomCreateError, pass_arguments=False,
    )
    def from_dict(
        cls,
        parsed_dict: Dict[Any, Any],
    ):
        # type: (...) -> Union[RoomCreateResponse, RoomCreateError]
        return cls(parsed_dict["room_id"])



class JoinResponse(RoomIdResponse):
    @staticmethod
    def create_error(parsed_dict):
        return JoinError.from_dict(parsed_dict)


class RoomLeaveResponse(EmptyResponse):
    @staticmethod
    def create_error(parsed_dict):
        return RoomLeaveError.from_dict(parsed_dict)


class RoomForgetResponse(_EmptyResponseWithRoomId):
    """Response representing a successful forget room request."""
    @staticmethod
    def create_error(parsed_dict, room_id):
        return RoomForgetError.from_dict(parsed_dict, room_id)


@dataclass
class KeysUploadResponse(Response):
    curve25519_count: int = field()
    signed_curve25519_count: int = field()

    @classmethod
    @verify(Schemas.keys_upload, KeysUploadError)
    def from_dict(cls, parsed_dict: Dict[Any, Any]):
        # type: (...) -> Union[KeysUploadResponse, ErrorResponse]
        counts = parsed_dict["one_time_key_counts"]
        return cls(counts["curve25519"], counts["signed_curve25519"])


@dataclass
class KeysQueryResponse(Response):
    device_keys: Dict = field()
    failures: Dict = field()
    changed: Dict[str, Dict[str, Any]] = field(
        init=False, default_factory=dict,
    )

    @classmethod
    @verify(Schemas.keys_query, KeysQueryError)
    def from_dict(cls, parsed_dict: Dict[Any, Any]):
        # type: (...) -> Union[KeysQueryResponse, ErrorResponse]
        device_keys = parsed_dict["device_keys"]
        failures = parsed_dict["failures"]

        return cls(device_keys, failures)


@dataclass
class KeysClaimResponse(Response):
    one_time_keys: Dict[Any, Any] = field()
    failures: Dict[Any, Any] = field()
    room_id: str = ""

    @classmethod
    @verify(Schemas.keys_claim, KeysClaimError)
    def from_dict(
        cls,
        parsed_dict: Dict[Any, Any],
        room_id: str = "",
    ):
        # type: (...) -> Union[KeysClaimResponse, ErrorResponse]
        one_time_keys = parsed_dict["one_time_keys"]
        failures = parsed_dict["failures"]

        return cls(one_time_keys, failures, room_id)


@dataclass
class DevicesResponse(Response):
    devices: List[Device] = field()

    @classmethod
    @verify(Schemas.devices, DevicesError)
    def from_dict(cls, parsed_dict: Dict[Any, Any]):
        # type: (...) -> Union[DevicesResponse, ErrorResponse]
        devices = []
        for device_dict in parsed_dict["devices"]:
            try:
                device = Device.from_dict(device_dict)
            except ValueError:
                continue
            devices.append(device)

        return cls(devices)


@dataclass
class RoomKeyRequestError(ErrorResponse):
    """Response representing a failed room key request."""

    pass


@dataclass
class RoomKeyRequestResponse(Response):
    """Response representing a successful room key request.

    Attributes:
        request_id (str): The id of the that uniquely identifies this key
            request that was requested, if we receive a to_device event it will
            contain the same request id.
        session_id (str): The id of the session that we requested.
        room_id (str): The id of the room that the session belongs to.
        algorithm (str): The encryption algorithm of the session.

    """

    request_id: str = field()
    session_id: str = field()
    room_id: str = field()
    algorithm: str = field()

    @classmethod
    @verify(Schemas.empty, RoomKeyRequestError, False)
    def from_dict(cls, _, request_id, session_id, room_id, algorithm):
        """Create a RoomKeyRequestResponse from a json response.

        Args:
            parsed_dict (Dict): The dictionary containing the json response.
            request_id (str): The id of that uniquely identifies this key
                request that was requested, if we receive a to_device event
                it will contain the same request id.
            session_id (str): The id of the session that we requested.
            room_id (str): The id of the room that the session belongs to.
            algorithm (str): The encryption algorithm of the session.
        """
        return cls(request_id, session_id, room_id, algorithm)


class UpdateDeviceResponse(EmptyResponse):
    @staticmethod
    def create_error(parsed_dict):
        return UpdateDeviceError.from_dict(parsed_dict)


@dataclass
class ProfileGetResponse(Response):
    """Response representing a successful get profile request.

    Attributes:
        displayname (str, optional): The display name of the user.
            None if the user doesn't have a display name.
        avatar_url (str, optional): The matrix content URI for the user's
            avatar. None if the user doesn't have an avatar.
        other_info (dict): Contains any other information returned for the
            user's profile.
    """

    displayname: Optional[str] = None
    avatar_url: Optional[str] = None
    other_info: Dict[Any, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return "Display name: {}, avatar URL: {}, other info: {}".format(
            self.displayname,
            self.avatar_url,
            self.other_info,
        )

    @classmethod
    @verify(Schemas.get_profile, ProfileGetError)
    def from_dict(cls, parsed_dict: Dict[Any, Any]):
        # type: (...) -> Union[ProfileGetResponse, ErrorResponse]
        return cls(
            parsed_dict.get("displayname"),
            parsed_dict.get("avatar_url"),
            {k: v for k, v in parsed_dict.items()
             if k not in ("displayname", "avatar_url")},
        )


@dataclass
class ProfileGetDisplayNameResponse(Response):
    """Response representing a successful get display name request.

    Attributes:
        displayname (str, optional): The display name of the user.
            None if the user doesn't have a display name.
    """

    displayname: Optional[str] = None

    def __str__(self) -> str:
        return "Display name: {}".format(self.displayname)

    @classmethod
    @verify(Schemas.get_displayname, ProfileGetDisplayNameError)
    def from_dict(
        cls,
        parsed_dict: (Dict[Any, Any]),
    ):
        # type: (...) -> Union[ProfileGetDisplayNameResponse, ErrorResponse]
        return cls(parsed_dict.get("displayname"))


class ProfileSetDisplayNameResponse(EmptyResponse):
    @staticmethod
    def create_error(parsed_dict):
        return ProfileSetDisplayNameError.from_dict(parsed_dict)


@dataclass
class ProfileGetAvatarResponse(Response):
    """Response representing a successful get avatar request.

    Attributes:
        avatar_url (str, optional): The matrix content URI for the user's
            avatar. None if the user doesn't have an avatar.
    """

    avatar_url: Optional[str] = None

    def __str__(self) -> str:
        return "Avatar URL: {}".format(self.avatar_url)

    @classmethod
    @verify(Schemas.get_avatar, ProfileGetAvatarError)
    def from_dict(
        cls,
        parsed_dict: (Dict[Any, Any]),
    ):
        # type: (...) -> Union[ProfileGetAvatarResponse, ErrorResponse]
        return cls(parsed_dict.get("avatar_url"))


class ProfileSetAvatarResponse(EmptyResponse):
    @staticmethod
    def create_error(parsed_dict):
        return ProfileSetAvatarError.from_dict(parsed_dict)


@dataclass
class PresenceGetResponse(Response):
    """Response representing a successful get presence request.

    Attributes:
        user_id (str): The user´s id
        presence (str): The user's presence state. One of: ["online",
            "offline", "unavailable"]
        last_active_ago (int, optional): The length of time in milliseconds
            since an action was performed by this user. None if not set.
        currently_active (bool, optional): Whether the user is currently
            active. None if not set.
        status_msg (str, optional): The state message for this user. None if
            not set.
    """

    user_id: str
    presence: str
    last_active_ago: Optional[int]
    currently_active: Optional[bool]
    status_msg: Optional[str]

    @classmethod
    @verify(Schemas.get_presence, PresenceGetError, pass_arguments=False)
    def from_dict(
        cls,
        parsed_dict: Dict[Any, Any],
        user_id: str
    ) -> Union["PresenceGetResponse", PresenceGetError]:
        return cls(
            user_id,
            parsed_dict.get("presence", "offline"),
            parsed_dict.get("last_active_ago"),
            parsed_dict.get("currently_active"),
            parsed_dict.get("status_msg")
        )


class PresenceSetResponse(EmptyResponse):
    """Response representing a successful set presence request."""

    @staticmethod
    def create_error(parsed_dict):
        return PresenceSetError.from_dict(parsed_dict)


@dataclass
class ToDeviceError(ErrorResponse):
    """Response representing a unsuccessful room key request."""

    to_device_message: Optional[ToDeviceMessage] = None

    @classmethod
    def from_dict(cls, parsed_dict, message):
        try:
            validate_json(parsed_dict, Schemas.error)
        except (SchemaError, ValidationError):
            return cls("unknown error", None, message)

        return cls(parsed_dict["error"], parsed_dict["errcode"], message)


@dataclass
class ToDeviceResponse(Response):
    """Response representing a successful room key request."""

    to_device_message: ToDeviceMessage = field()

    @classmethod
    @verify(Schemas.empty, ToDeviceError)
    def from_dict(cls, parsed_dict, message):
        """Create a ToDeviceResponse from a json response."""
        return cls(message)


@dataclass
class RoomContextError(_ErrorWithRoomId):
    """Response representing a unsuccessful room context request."""


@dataclass
class RoomContextResponse(Response):
    """Room event context response.

    This Response holds a number of events that happened just before and after
    a specified event.

    Attributes:
        room_id(str): The room id of the room which the events belong to.
        start(str): A token that can be used to paginate backwards with.
        end(str): A token that can be used to paginate forwards with.
        events_before(List[Event]): A list of room events that happened just
            before the requested event, in reverse-chronological order.
        event(Event): Details of the requested event.
        events_after(List[Event]): A list of room events that happened just
            after the requested event, in chronological order.
        state(List[Event]): The state of the room at the last event returned.

    """

    room_id: str = field()

    start: str = field()
    end: str = field()

    event: Optional[Union[Event, BadEventType]] = field()

    events_before: List[Union[Event, BadEventType]] = field()
    events_after: List[Union[Event, BadEventType]] = field()

    state: List[Union[Event, BadEventType]] = field()

    @classmethod
    @verify(Schemas.room_context, RoomContextError)
    def from_dict(
        cls,
        parsed_dict: Dict[Any, Any],
        room_id: str,
    ):
        # type: (...) -> Union[RoomContextResponse, ErrorResponse]
        events_before = SyncResponse._get_room_events(
            parsed_dict["events_before"]
        )
        events_after = SyncResponse._get_room_events(
            parsed_dict["events_after"]
        )
        event = Event.parse_event(parsed_dict["event"])

        state = SyncResponse._get_room_events(
            parsed_dict["state"]
        )

        return cls(room_id, parsed_dict["start"], parsed_dict["end"],
                   event, events_before, events_after, state)


@dataclass
class SyncResponse(Response):
    next_batch: str = field()
    rooms: Rooms = field()
    device_key_count: DeviceOneTimeKeyCount = field()
    device_list: DeviceList = field()
    to_device_events: List[ToDeviceEvent] = field()
    presence_events: List[PresenceEvent] = field()
    account_data_events: List[AccountDataEvent] = field(default_factory=list)

    def __str__(self) -> str:
        result = []
        for room_id, room_info in self.rooms.join.items():
            room_header = "  Messages for room {}:\n    ".format(room_id)
            messages = []
            for event in room_info.timeline.events:
                messages.append(str(event))

            room_message = room_header + "\n    ".join(messages)
            result.append(room_message)

        if len(self.to_device_events) > 0:
            result.append("  Device messages:")
            for event in self.to_device_events:
                result.append("    {}".format(event))

        body = "\n".join(result)
        string = ("Sync response until batch: {}:\n{}").format(
            self.next_batch, body
        )
        return string

    @staticmethod
    def _get_room_events(parsed_dict: List[Dict[Any, Any]]) -> List[Union[Event, BadEventType]]:
        events: List[Union[Event, BadEventType]] = []

        for event_dict in parsed_dict:
            event = Event.parse_event(event_dict)

            if event:
                events.append(event)

        return events

    @staticmethod
    def _get_to_device(parsed_dict: Dict[Any, Any]):
        # type: (...) -> List[ToDeviceEvent]
        events: List[ToDeviceEvent] = []
        for event_dict in parsed_dict["events"]:
            event = ToDeviceEvent.parse_event(event_dict)

            if event:
                events.append(event)

        return events

    @staticmethod
    def _get_timeline(parsed_dict: Dict[Any, Any]) -> Timeline:
        validate_json(parsed_dict, Schemas.room_timeline)

        events = SyncResponse._get_room_events(parsed_dict["events"])

        return Timeline(
            events, parsed_dict["limited"], parsed_dict["prev_batch"]
        )

    @staticmethod
    def _get_state(parsed_dict: Dict[Any, Any]) -> List[Union[Event, BadEventType]]:
        validate_json(parsed_dict, Schemas.sync_room_state)
        events = SyncResponse._get_room_events(
            parsed_dict["events"],
        )

        return events

    @staticmethod
    def _get_invite_state(parsed_dict):
        validate_json(parsed_dict, Schemas.sync_room_state)
        events = []

        for event_dict in parsed_dict["events"]:
            event = InviteEvent.parse_event(event_dict)

            if event:
                events.append(event)

        return events

    @staticmethod
    def _get_ephemeral_events(parsed_dict):
        events = []
        for event_dict in parsed_dict:
            event = EphemeralEvent.parse_event(event_dict)

            if event:
                events.append(event)
        return events

    @staticmethod
    def _get_join_info(
        state_events: List[Any],
        timeline_events: List[Any],
        prev_batch: str,
        limited: bool,
        ephemeral_events: List[Any],
        summary_events: Dict[str, Any],
        unread_notification_events: Dict[str, Any],
        account_data_events: List[Any],
    ) -> RoomInfo:
        state = SyncResponse._get_room_events(state_events)

        events = SyncResponse._get_room_events(timeline_events)
        timeline = Timeline(events, limited, prev_batch)

        ephemeral_event_list = SyncResponse._get_ephemeral_events(
            ephemeral_events
        )

        summary = RoomSummary(
            summary_events.get("m.invited_member_count"),
            summary_events.get("m.joined_member_count"),
            summary_events.get("m.heroes"),
        )

        unread_notifications = UnreadNotifications(
            unread_notification_events.get("notification_count"),
            unread_notification_events.get("highlight_count"),
        )

        account_data = RoomInfo.parse_account_data(account_data_events)

        return RoomInfo(
            timeline,
            state,
            ephemeral_event_list,
            account_data,
            summary,
            unread_notifications,
        )

    @staticmethod
    def _get_room_info(parsed_dict: Dict[Any, Any]) -> Rooms:
        joined_rooms: Dict[str, RoomInfo] = {}
        invited_rooms: Dict[str, InviteInfo] = {}
        left_rooms: Dict[str, RoomInfo] = {}

        for room_id, room_dict in parsed_dict["invite"].items():
            state = SyncResponse._get_invite_state(room_dict["invite_state"])
            invite_info = InviteInfo(state)
            invited_rooms[room_id] = invite_info

        for room_id, room_dict in parsed_dict["leave"].items():
            state = SyncResponse._get_state(room_dict["state"])
            timeline = SyncResponse._get_timeline(room_dict["timeline"])
            leave_info = RoomInfo(timeline, state, [], [])
            left_rooms[room_id] = leave_info

        for room_id, room_dict in parsed_dict["join"].items():
            join_info = SyncResponse._get_join_info(
                room_dict["state"]["events"],
                room_dict["timeline"]["events"],
                room_dict["timeline"]["prev_batch"],
                room_dict["timeline"]["limited"],
                room_dict["ephemeral"]["events"],
                room_dict.get("summary", {}),
                room_dict.get("unread_notifications", {}),
                room_dict["account_data"]["events"],
            )

            joined_rooms[room_id] = join_info

        return Rooms(invited_rooms, joined_rooms, left_rooms)

    @staticmethod
    def _get_presence(parsed_dict) -> List[PresenceEvent]:
        presence_events = []
        for presence_dict in parsed_dict.get("presence", {}).get("events", []):
            presence_events.append(PresenceEvent.from_dict(presence_dict))

        return presence_events

    @staticmethod
    def _get_account_data(
        parsed_dict: Dict[str, Any],
    ) -> Generator[AccountDataEvent, None, None]:
        for ev_dict in parsed_dict.get("account_data", {}).get("events", []):
            yield AccountDataEvent.parse_event(ev_dict)

    @classmethod
    @verify(Schemas.sync, SyncError, False)
    def from_dict(
        cls,
        parsed_dict: Dict[Any, Any],
    ):
        # type: (...) -> Union[SyncResponse, ErrorResponse]
        to_device = cls._get_to_device(parsed_dict["to_device"])

        key_count_dict = parsed_dict["device_one_time_keys_count"]
        key_count = DeviceOneTimeKeyCount(
            key_count_dict.get("curve25519"),
            key_count_dict.get("signed_curve25519")
        )

        devices = DeviceList(
            parsed_dict.get("device_lists", {}).get("changed", []),
            parsed_dict.get("device_lists", {}).get("left", []),
        )

        presence_events = SyncResponse._get_presence(parsed_dict)

        rooms = SyncResponse._get_room_info(parsed_dict["rooms"])

        return SyncResponse(
            parsed_dict["next_batch"],
            rooms,
            key_count,
            devices,
            to_device,
            presence_events,
            list(SyncResponse._get_account_data(parsed_dict)),
        )


class UploadFilterError(ErrorResponse):
    pass


@dataclass
class UploadFilterResponse(Response):
    """Response representing a successful filter upload request.

    Attributes:
        filter_id (str): A filter ID that may be used in
            future requests to restrict which events are returned to the
            client.
    """
    filter_id: str = field()

    @classmethod
    @verify(Schemas.upload_filter, UploadFilterError)
    def from_dict(
        cls, parsed_dict: Dict[Any, Any],
    ) -> Union["UploadFilterResponse", UploadFilterError]:
        return cls(parsed_dict["filter_id"])


class WhoamiError(ErrorResponse):
    pass


@dataclass
class WhoamiResponse(Response):

    user_id: str = field()

    @classmethod
    @verify(Schemas.whoami, WhoamiError)
    def from_dict(
        cls, parsed_dict: Dict[Any, Any],
    ) -> Union["WhoamiResponse", WhoamiError]:
        return cls(parsed_dict["user_id"])


@dataclass
class SetPushRuleResponse(EmptyResponse):
    @staticmethod
    def create_error(parsed_dict: Dict[str, Any]):
        return SetPushRuleError.from_dict(parsed_dict)


class SetPushRuleError(ErrorResponse):
    pass


@dataclass
class DeletePushRuleResponse(EmptyResponse):
    @staticmethod
    def create_error(parsed_dict: Dict[str, Any]):
        return DeletePushRuleError.from_dict(parsed_dict)


class DeletePushRuleError(ErrorResponse):
    pass


@dataclass
class EnablePushRuleResponse(EmptyResponse):
    @staticmethod
    def create_error(parsed_dict: Dict[str, Any]):
        return EnablePushRuleError.from_dict(parsed_dict)


class EnablePushRuleError(ErrorResponse):
    pass


@dataclass
class SetPushRuleActionsResponse(EmptyResponse):
    @staticmethod
    def create_error(parsed_dict: Dict[str, Any]):
        return SetPushRuleActionsError.from_dict(parsed_dict)


class SetPushRuleActionsError(ErrorResponse):
    pass


@dataclass
class ConnectionInfo:
    ip: Optional[str] = field()
    last_seen: Optional[int] = field()
    user_agent: Optional[str] = field()

    @classmethod
    def from_dict(cls, parsed_dict: Dict[str, Any]) -> "ConnectionInfo":
        return cls(parsed_dict.get("ip", None),
                   parsed_dict.get("last_seen", None),
                   parsed_dict.get("user_agent", None))


@dataclass
class SessionInfo:
    connections: List[ConnectionInfo] = field()

    @classmethod
    def from_dict(cls, parsed_dict: Dict[str, Any]) -> "SessionInfo":
        return cls([
            ConnectionInfo.from_dict(c)
            for c in parsed_dict.get("connections", [])
        ])


@dataclass
class DeviceInfo:
    sessions: List[SessionInfo] = field()

    @classmethod
    def from_dict(cls, parsed_dict: Dict[str, Any]) -> "DeviceInfo":
        return cls([
            SessionInfo.from_dict(s) for s in parsed_dict.get("sessions", [])
        ])


class WhoisError(ErrorResponse):
    pass


@dataclass
class WhoisResponse(Response):
    user_id: Optional[str] = field()
    devices: Dict[str, DeviceInfo] = field()

    @classmethod
    @verify(Schemas.whois, WhoisError)
    def from_dict(
            cls, parsed_dict: Dict[str,
                                   Any]) -> Union["WhoisResponse", WhoisError]:
        return cls(
            parsed_dict.get("user_id", None), {
                dk: DeviceInfo.from_dict(dv)
                for dk, dv in parsed_dict.get("devices", dict()).items()
            })
