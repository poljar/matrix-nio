# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import json

from nio.responses import (DeleteDevicesAuthResponse, DevicesResponse,
                           DownloadResponse, DownloadError,
                           ErrorResponse, JoinedMembersError,
                           JoinedMembersResponse, JoinResponse,
                           KeysClaimResponse,
                           KeysQueryResponse, KeysUploadResponse, LoginError,
                           LoginResponse, LogoutError, LogoutResponse,
                           ProfileGetAvatarResponse,
                           ProfileGetDisplayNameResponse, ProfileGetResponse,
                           RoomContextError, RoomContextResponse,
                           RoomCreateResponse, RoomForgetResponse,
                           RoomKeyRequestError, RoomKeyRequestResponse,
                           RoomLeaveResponse, RoomMessagesResponse,
                           RoomTypingResponse, SyncError,
                           SyncResponse, ThumbnailResponse, ThumbnailError,
                           ToDeviceError, ToDeviceResponse,
                           UploadResponse, _ErrorWithRoomId, LoginInfoResponse,
                           WhoisResponse, ConnectionInfo, DeviceInfo,
                           SessionInfo)

TEST_ROOM_ID = "!test:example.org"


class TestClass:
    @staticmethod
    def _load_bytes(filename):
        with open(filename, "rb") as f:
            return f.read()

    @staticmethod
    def _load_response(filename):
        with open(filename) as f:
            return json.loads(f.read())

    def test_login_parse(self):
        parsed_dict = TestClass._load_response(
            "tests/data/login_response.json")
        response = LoginResponse.from_dict(parsed_dict)
        assert isinstance(response, LoginResponse)

    def test_login_failure_parse(self):
        parsed_dict = TestClass._load_response(
            "tests/data/login_response_error.json")
        response = LoginResponse.from_dict(parsed_dict)
        assert isinstance(response, LoginError)

    def test_login_failure_format(self):
        parsed_dict = TestClass._load_response(
            "tests/data/login_invalid_format.json")
        response = LoginResponse.from_dict(parsed_dict)
        assert isinstance(response, ErrorResponse)

    def test_logout_parse(self):
        parsed_dict = TestClass._load_response(
            "tests/data/logout_response.json")
        response = LogoutResponse.from_dict(parsed_dict)
        assert isinstance(response, LogoutResponse)

    def test_room_messages(self):
        parsed_dict = TestClass._load_response(
            "tests/data/room_messages.json")
        response = RoomMessagesResponse.from_dict(parsed_dict, TEST_ROOM_ID)
        assert isinstance(response, RoomMessagesResponse)

    def test_keys_upload(self):
        parsed_dict = TestClass._load_response(
            "tests/data/keys_upload.json")
        response = KeysUploadResponse.from_dict(parsed_dict)
        assert isinstance(response, KeysUploadResponse)

    def test_keys_query(self):
        parsed_dict = TestClass._load_response(
            "tests/data/keys_query.json")
        response = KeysQueryResponse.from_dict(parsed_dict)
        assert isinstance(response, KeysQueryResponse)

    def test_keys_claim(self):
        parsed_dict = TestClass._load_response(
            "tests/data/keys_claim.json")
        response = KeysClaimResponse.from_dict(
            parsed_dict,
            "!test:example.org"
        )
        assert isinstance(response, KeysClaimResponse)

    def test_devices(self):
        parsed_dict = TestClass._load_response(
            "tests/data/devices.json")
        response = DevicesResponse.from_dict(parsed_dict)
        assert isinstance(response, DevicesResponse)
        assert response.devices[0].id == "QBUAZIFURK"

    def test_delete_devices_auth(self):
        parsed_dict = TestClass._load_response(
            "tests/data/delete_devices.json")
        response = DeleteDevicesAuthResponse.from_dict(parsed_dict)
        assert isinstance(response, DeleteDevicesAuthResponse)
        assert response.session == "xxxxxxyz"

    def test_joined_parse(self):
        parsed_dict = TestClass._load_response(
            "tests/data/joined_members_response.json")
        response = JoinedMembersResponse.from_dict(parsed_dict, "!testroom")
        assert isinstance(response, JoinedMembersResponse)

    def test_joined_fail(self):
        parsed_dict = {}
        response = JoinedMembersResponse.from_dict(parsed_dict, "!testroom")
        assert isinstance(response, JoinedMembersError)

    def test_upload_parse(self):
        parsed_dict = TestClass._load_response(
            "tests/data/upload_response.json")
        response = UploadResponse.from_dict(parsed_dict)
        assert isinstance(response, UploadResponse)

    def test_download(self):
        data = TestClass._load_bytes("tests/data/file_response")
        response = DownloadResponse.from_data(data, "image/png", "example.png")
        assert isinstance(response, DownloadResponse)
        assert response.body == data
        assert response.content_type == "image/png"
        assert response.filename == "example.png"

        data = TestClass._load_response("tests/data/limit_exceeded_error.json")
        response = DownloadResponse.from_data(data, "image/png")
        assert isinstance(response, DownloadError)
        assert response.status_code == data["errcode"]

        response = DownloadResponse.from_data("123", "image/png")
        assert isinstance(response, DownloadError)

    def test_thumbnail(self):
        data = TestClass._load_bytes("tests/data/file_response")
        response = ThumbnailResponse.from_data(data, "image/png")
        assert isinstance(response, ThumbnailResponse)
        assert response.body == data

        data = TestClass._load_response("tests/data/limit_exceeded_error.json")
        response = ThumbnailResponse.from_data(data, "image/png")
        assert isinstance(response, ThumbnailError)
        assert response.status_code == data["errcode"]

        response = ThumbnailResponse.from_data("123", "image/png")
        assert isinstance(response, ThumbnailError)

        response = ThumbnailResponse.from_data(b"5xx error", "text/html")
        assert isinstance(response, ThumbnailError)

    def test_sync_fail(self):
        parsed_dict = {}
        response = SyncResponse.from_dict(parsed_dict, 0)
        assert isinstance(response, SyncError)

    def test_sync_parse(self):
        parsed_dict = TestClass._load_response(
            "tests/data/sync.json")
        response = SyncResponse.from_dict(parsed_dict)
        assert type(response) == SyncResponse

    def test_keyshare_request(self):
        parsed_dict = {
            "errcode": "M_LIMIT_EXCEEDED",
            "error": "Too many requests",
            "retry_after_ms": 2000
        }
        response = RoomKeyRequestResponse.from_dict(
            parsed_dict, "1", "1", TEST_ROOM_ID, "megolm.v1"
        )
        assert isinstance(response, RoomKeyRequestError)
        response = RoomKeyRequestResponse.from_dict(
                {}, "1", "1", TEST_ROOM_ID, "megolm.v1"
        )
        assert isinstance(response, RoomKeyRequestResponse)

    def test_get_profile(self):
        parsed_dict = TestClass._load_response(
            "tests/data/get_profile_response.json")
        response = ProfileGetResponse.from_dict(parsed_dict)
        assert isinstance(response, ProfileGetResponse)
        assert response.other_info == {"something_else": 123}

    def test_get_displayname(self):
        parsed_dict = TestClass._load_response(
            "tests/data/get_displayname_response.json")
        response = ProfileGetDisplayNameResponse.from_dict(parsed_dict)
        assert isinstance(response, ProfileGetDisplayNameResponse)

    def test_get_avatar(self):
        parsed_dict = TestClass._load_response(
            "tests/data/get_avatar_response.json")
        response = ProfileGetAvatarResponse.from_dict(parsed_dict)
        assert isinstance(response, ProfileGetAvatarResponse)

    def test_to_device(self):
        message = "message"
        response = ToDeviceResponse.from_dict(
            {"error": "error", "errcode": "M_UNKNOWN"}, message
        )
        assert isinstance(response, ToDeviceError)
        response = ToDeviceResponse.from_dict({}, message)
        assert isinstance(response, ToDeviceResponse)

    def test_context(self):
        response = RoomContextResponse.from_dict(
            {"error": "error", "errcode": "M_UNKNOWN"}, TEST_ROOM_ID
        )
        assert isinstance(response, RoomContextError)
        assert response.room_id == TEST_ROOM_ID

        parsed_dict = TestClass._load_response("tests/data/context.json")
        response = RoomContextResponse.from_dict(parsed_dict, TEST_ROOM_ID)

        assert isinstance(response, RoomContextResponse)

        assert response.room_id == TEST_ROOM_ID
        assert not response.events_before
        assert len(response.events_after) == 1
        assert len(response.state) == 9

    def test_limit_exceeded_error(self):
        parsed_dict = TestClass._load_response(
            "tests/data/limit_exceeded_error.json")

        response = ErrorResponse.from_dict(parsed_dict)
        assert isinstance(response, ErrorResponse)
        assert response.retry_after_ms == parsed_dict["retry_after_ms"]

        room_id = "!SVkFJHzfwvuaIEawgC:localhost"
        response2 = _ErrorWithRoomId.from_dict(parsed_dict, room_id)
        assert isinstance(response2, _ErrorWithRoomId)
        assert response.retry_after_ms == parsed_dict["retry_after_ms"]
        assert response2.room_id == room_id

    def test_room_create(self):
        parsed_dict = TestClass._load_response(
            "tests/data/room_id.json")
        response = RoomCreateResponse.from_dict(parsed_dict)
        assert isinstance(response, RoomCreateResponse)

    def test_join(self):
        parsed_dict = TestClass._load_response(
            "tests/data/room_id.json")
        response = JoinResponse.from_dict(parsed_dict)
        assert isinstance(response, JoinResponse)

    def test_room_leave(self):
        response = RoomLeaveResponse.from_dict({})
        assert isinstance(response, RoomLeaveResponse)

    def test_room_forget(self):
        response = RoomForgetResponse.from_dict({}, TEST_ROOM_ID)
        assert isinstance(response, RoomForgetResponse)

    def test_room_typing(self):
        response = RoomTypingResponse.from_dict({}, TEST_ROOM_ID)
        assert isinstance(response, RoomTypingResponse)

    def test_login_info(self):
        parsed_dict = TestClass._load_response(
            "tests/data/login_info.json")
        response = LoginInfoResponse.from_dict(parsed_dict)
        assert isinstance(response, LoginInfoResponse)

    def test_whois_parse(self):
        parsed_dict = TestClass._load_response(
            "tests/data/whois_response.json")
        response = WhoisResponse.from_dict(parsed_dict)
        assert isinstance(response, WhoisResponse)
        device = next(iter(response.devices.values()))
        assert isinstance(device, DeviceInfo)
        session = device.sessions[0]
        assert isinstance(session, SessionInfo)
        connection = session.connections[0]
        assert isinstance(connection, ConnectionInfo)

    def test_whois_parse_empty(self):
        parsed_dict = dict()
        response = WhoisResponse.from_dict(parsed_dict)
        assert isinstance(response, WhoisResponse)
