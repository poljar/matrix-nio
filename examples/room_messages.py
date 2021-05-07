#!/usr/bin/env python3

# Read all messages in the specified room. This example shows how you can use
# AsyncClient.room_messages()

import nio
import asyncio

class config:
    homeserver = ""
    user = ""
    password = ""
    room_id = ""
    device_id = "Example bot"

class matrix_client:
    past_tokens = {}
    client = nio.AsyncClient(config.homeserver, config.user, device_id=config.device_id)

    # The sync API returns "prev_batch" tokens for each room. We need those to
    # call client.room_messages()
    async def sync_response(self, resp: nio.SyncResponse):
        for room_id, info in resp.rooms.join.items():
            if room_id not in self.past_tokens:
                 self.past_tokens[room_id] = info.timeline.prev_batch

    # Read all messages beginning from the past_token to oldest or newest
    # message (depending on the direction)
    async def read_all_events_in_direction(self, direction):
        all_events = []

        current_token = self.past_tokens[config.room_id]
        while True:
            events = await self.client.room_messages(config.room_id, current_token, limit = 500, direction = direction)
            print("Received", len(events.chunk), "events")
            current_token = events.end

            if len(events.chunk) == 0:
                break

            all_events = all_events + events.chunk
        return all_events

    async def read_all_events(self):
        # Wait for sync_response() to set self.past_tokens[config.room_id].
        # This is necessary for read_all_events_in_direction()
        await self.client.synced.wait()

        back_events = await self.read_all_events_in_direction(nio.MessageDirection.back)
        front_events = await self.read_all_events_in_direction(nio.MessageDirection.front)

        # We have to reverse the first list since we are going backwards (but
        # we want to have a chronological order)
        all_events = back_events[::-1] + front_events
        return all_events

    async def close(self):
        await self.client.close()
        self.task.cancel()

    async def read_all_messages(self):
        for event in await self.read_all_events():
            if isinstance(event, nio.events.room_events.RoomMessageText):
                print(event.body)

        await self.close()

    async def main(self):
        self.client.add_response_callback(self.sync_response, nio.SyncResponse)

        print(await self.client.login(config.password))

        self.task = asyncio.gather(
            self.read_all_messages(),
            self.client.sync_forever(timeout=30000)
        )
        await self.task

try:
    asyncio.get_event_loop().run_until_complete(matrix_client().main())
except asyncio.exceptions.CancelledError:
    pass
finally:
    asyncio.get_event_loop().close()
