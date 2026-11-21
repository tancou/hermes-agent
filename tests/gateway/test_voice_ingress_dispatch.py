"""Real plugin-command dispatch -> continuous STT -> ordinary Discord voice event."""

import functools
import weakref
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

pytest.importorskip("discord")

from agent.continuous_transcription import TranscriptionEvent
from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.platforms.event import MessageEvent, MessageType
from gateway.run import GatewayRunner
from plugins.platforms.discord.adapter import DiscordAdapter
from tests.gateway.test_voice_ingress import Room
from tests.waiting import async_wait_until


@pytest.mark.asyncio
@pytest.mark.parametrize("offloaded", [False, True])
async def test_command_captures_factory_and_transcript_reenters_normal_voice(tmp_path, monkeypatch, offloaded):
    monkeypatch.setenv("DISCORD_ALLOW_ALL_USERS", "true")
    monkeypatch.setenv("DISCORD_ALLOWED_USERS", "42")
    home = tmp_path / "profile"
    home.mkdir()
    room = Room(home)
    room.guild.get_member = lambda uid: SimpleNamespace(id=uid, display_name=f"member-{uid}")
    adapter = DiscordAdapter(PlatformConfig(enabled=True, token="fake"))
    adapter._allowed_user_ids = {"42"}
    adapter._voice_clients = room.adapter._voice_clients
    adapter._voice_text_channels = {1: 700}
    adapter._voice_sources = {1: room.source.to_dict()}
    adapter._client = SimpleNamespace(get_channel=lambda _: None, get_guild=lambda _: room.guild)
    adapter._reset_voice_timeout = lambda _: None
    adapter.handle_message = AsyncMock()
    runner = object.__new__(GatewayRunner)
    runner.adapters = {Platform.DISCORD: adapter}
    runner.config = GatewayConfig()
    runner._voice_mode, runner._session_db, runner.session_store = {}, None, None
    runner._get_executor = lambda: None
    runner._profile_scope_for_source = room._profile_scope_for_source
    room.source._transport_adapter_ref = weakref.ref(adapter)
    adapter._voice_input_callback = functools.partial(runner._handle_voice_channel_input, adapter=adapter)
    captures = []

    async def handler(raw, invocation=None):
        captures.append(invocation)
        factory = invocation.capture_realtime_voice_attachment_factory()
        captures.append(await factory.open("continuous-test"))
        return "canonical ready"

    def sync_handler(raw, invocation=None):
        # A synchronous plugin handler returns a coroutine to the gateway loop,
        # as Hermes Talk does. Its context must survive both thread hops.
        return handler(raw, invocation)

    event = MessageEvent(source=room.source, text="/talk core join", message_type=MessageType.TEXT)
    reply = await runner._hm_run_plugin_handler(sync_handler if offloaded else handler, event, room.source)
    assert reply == "canonical ready"
    assert captures[0].active is False
    attachment = captures[1]
    try:
        await attachment.feed_audio(b"\x01\x00", speaker_user_id=42)
        room.provider.sessions[0].queue.put_nowait(
            TranscriptionEvent("transcript", "u1", "inspect the deployment status", True))
        def delivered():
            if attachment._failure is not None:
                raise attachment._failure
            return adapter.handle_message.await_count == 1
        await async_wait_until(delivered)
        normal_event = adapter.handle_message.await_args.args[0]
        assert normal_event.message_type == MessageType.VOICE
        assert normal_event.text == "inspect the deployment status"
        assert normal_event.source.user_id == "42"
        assert normal_event.source.chat_id == "700"
        assert normal_event.raw_message.guild_id == 1
        with pytest.raises(RuntimeError, match="expired"):
            captures[0].capture_realtime_voice_attachment_factory()
    finally:
        await attachment.close()
