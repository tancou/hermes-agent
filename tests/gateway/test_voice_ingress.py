"""Continuous STT enters canonical voice with real config/registry/profile scopes."""

import asyncio
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from agent.continuous_transcription import ContinuousTranscriptionSession, TranscriptionEvent
from agent.transcription_provider import TranscriptionProvider
from agent.transcription_registry import register_provider
from gateway.config import Platform
from gateway.plugin_voice import PluginVoiceInvocation, VoiceIngressFactory
from gateway.session import SessionSource
from hermes_constants import get_hermes_home, reset_hermes_home_override, set_hermes_home_override
from tests.waiting import async_wait_until


class Session(ContinuousTranscriptionSession):
    def __init__(self):
        self.queue = asyncio.Queue()
        self.queue.put_nowait(TranscriptionEvent("ready"))
        self.audio = []
        self.closed = False

    async def send_audio(self, pcm):
        self.audio.append(pcm)

    async def events(self):
        while True:
            item = await self.queue.get()
            if isinstance(item, Exception):
                raise item
            yield item

    async def close(self):
        self.closed = True


class Provider(TranscriptionProvider):
    name = "continuous-test"
    continuous_capable = True

    def __init__(self):
        self.sessions, self.homes = [], []

    def transcribe(self, file_path, **kwargs):
        raise AssertionError("The file transcription path must not run")

    async def open_continuous_session(self):
        self.homes.append(get_hermes_home())
        session = Session()
        self.sessions.append(session)
        return session


class Room:
    def __init__(self, home):
        self.home = home
        self.allowed = {42, 43}
        self.delivered = []
        self.guild = SimpleNamespace(id=1)
        self.channel = SimpleNamespace(id=90, guild=self.guild, members=[
            SimpleNamespace(id=n, bot=False) for n in (42, 43, 44)])
        self.voice = SimpleNamespace(channel=self.channel, is_connected=lambda: True)
        self.adapter = SimpleNamespace(
            _voice_clients={1: self.voice}, _voice_text_channels={1: 700},
            _voice_input_callback=self.deliver, _reset_voice_timeout=lambda _: None,
            _is_allowed_user=lambda uid, **_: int(uid) in self.allowed,
        )
        self.source = SessionSource(Platform.DISCORD, "700", user_id="42", guild_id="1")
        self.provider = Provider()
        register_provider(self.provider, scope=str(home))

    @contextmanager
    def _profile_scope_for_source(self, _source):
        token = set_hermes_home_override(self.home)
        try:
            yield
        finally:
            reset_hermes_home_override(token)

    def _intake_adapter_for(self, _source):
        return self.adapter

    def _is_user_authorized_for_source(self, source):
        return int(source.user_id) in self.allowed

    def _under_authorization_profile(self, _source, check):
        return check()

    async def deliver(self, **kwargs):
        self.delivered.append((get_hermes_home(), kwargs))


@pytest.fixture
def room(tmp_path):
    home = tmp_path / "profile"
    home.mkdir()
    return Room(home)


def configure(room, value):
    from hermes_cli.config import atomic_config_write
    with room._profile_scope_for_source(room.source):
        atomic_config_write(room.home / "config.yaml", {"discord": {"voice_operator_only": value}})


@pytest.mark.asyncio
@pytest.mark.parametrize("operator_only", [True, False])
async def test_speakers_keep_their_identity_and_profile(room, operator_only):
    configure(room, operator_only)
    factory = VoiceIngressFactory(room, room.source)
    attachment = await factory.open("continuous-test")
    try:
        assert await attachment.feed_audio(b"\x01\x00", speaker_user_id=42) == "accepted"
        outcome = await attachment.feed_audio(b"\x02\x00", speaker_user_id=43)
        assert outcome == ("ignored" if operator_only else "accepted")
        assert await attachment.feed_audio(b"\x03\x00", speaker_user_id=44) == "ignored"
        assert await attachment.feed_audio(b"\x03\x00", speaker_user_id=True) == "ignored"
        first = room.provider.sessions[0]
        first.queue.put_nowait(TranscriptionEvent("transcript", "partial", "unfinished"))
        first.queue.put_nowait(TranscriptionEvent("transcript", "one", "run the diagnostics", True))
        first.queue.put_nowait(TranscriptionEvent("transcript", "one", "run the diagnostics", True))
        count = 1
        if not operator_only:
            second = room.provider.sessions[1]
            second.queue.put_nowait(TranscriptionEvent("transcript", "one", "check my task too", True))
            count = 2
            assert [chunk for chunk in first.audio if any(chunk)] == [b"\x01\x00"]
            assert [chunk for chunk in second.audio if any(chunk)] == [b"\x02\x00"]
            assert first.audio[-1] == second.audio[-1] == b"\x00\x00"
        await async_wait_until(lambda: len(room.delivered) == count)
        assert {d[1]["user_id"] for d in room.delivered} == ({42} if operator_only else {42, 43})
        assert all(home == room.home for home, _ in room.delivered)
        assert all(home == room.home for home in room.provider.homes)
    finally:
        await attachment.close()
    assert all(s.closed for s in room.provider.sessions)
    assert not room.adapter._continuous_voice_sessions
    assert attachment._delivery.done() and all(t.done() for t in attachment._readers)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["text", "voice", "connection", "permission", "membership"])
async def test_pending_transcript_cannot_survive_revocation(room, change):
    attachment = await VoiceIngressFactory(room, room.source).open("continuous-test")
    try:
        changes = {
            "text": lambda: room.adapter._voice_text_channels.update({1: 701}),
            "voice": lambda: setattr(room.channel, "id", 91),
            "connection": lambda: room.adapter._voice_clients.update({1: SimpleNamespace()}),
            "permission": lambda: room.allowed.remove(42),
            "membership": room.channel.members.clear,
        }
        changes[change]()
        session = room.provider.sessions[0]
        session.queue.put_nowait(TranscriptionEvent("transcript", "one", "do not execute", True))
        session.queue.put_nowait(RuntimeError("end of scripted stream"))
        await async_wait_until(lambda: attachment._failure is not None)
        assert room.delivered == []
    finally:
        await attachment.close()


@pytest.mark.asyncio
async def test_failure_duplicate_conflict_and_single_room_lease(room):
    factory = VoiceIngressFactory(room, room.source)
    attachment = await factory.open("continuous-test")
    try:
        with pytest.raises(RuntimeError, match="already"):
            await factory.open("continuous-test")
        with pytest.raises(RuntimeError, match="already"):
            await VoiceIngressFactory(room, room.source).open("continuous-test")
        session = room.provider.sessions[0]
        session.queue.put_nowait(TranscriptionEvent("transcript", "one", "first", True))
        await async_wait_until(lambda: len(room.delivered) == 1)
        session.queue.put_nowait(TranscriptionEvent("transcript", "one", "different", True))
        await async_wait_until(lambda: attachment._failure is not None)
        with pytest.raises(RuntimeError, match="failed"):
            await attachment.feed_synthesized_silence(b"\x00\x00")
        assert len(room.delivered) == 1
    finally:
        await attachment.close()


@pytest.mark.asyncio
async def test_control_requires_same_room_and_owner_unless_opted_out(room):
    attachment = await VoiceIngressFactory(room, room.source).open("continuous-test")
    try:
        invocation = PluginVoiceInvocation(room, room.source)
        invocation.authorize_realtime_voice_control(guild_id=1)
        with pytest.raises(PermissionError):
            invocation.authorize_realtime_voice_control(guild_id=2)
        other = SessionSource(Platform.DISCORD, "700", user_id="43", guild_id="1")
        with pytest.raises(PermissionError):
            PluginVoiceInvocation(room, other).authorize_realtime_voice_control(guild_id=1)
        invocation.active = False
        with pytest.raises(RuntimeError, match="expired"):
            invocation.capture_realtime_voice_attachment_factory()
    finally:
        await attachment.close()


@pytest.mark.parametrize("value", ["false", 0, None])
def test_malformed_policy_never_disables_operator_gate(room, value):
    configure(room, value)
    with pytest.raises(ValueError, match="boolean"):
        VoiceIngressFactory(room, room.source)


@pytest.mark.asyncio
async def test_failed_start_closes_socket_and_releases_room(room):
    async def open_failed():
        session = Session()
        session.queue.get_nowait()
        session.queue.put_nowait(RuntimeError("provider failed before ready"))
        room.provider.sessions.append(session)
        return session

    room.provider.open_continuous_session = open_failed
    with pytest.raises(RuntimeError, match="failed"):
        await VoiceIngressFactory(room, room.source).open("continuous-test")
    assert room.provider.sessions[0].closed
    assert not room.adapter._continuous_voice_sessions


@pytest.mark.asyncio
async def test_relaxed_policy_allows_authorized_room_member_to_control(room):
    configure(room, False)
    attachment = await VoiceIngressFactory(room, room.source).open("continuous-test")
    try:
        other = SessionSource(Platform.DISCORD, "700", user_id="43", guild_id="1")
        PluginVoiceInvocation(room, other).authorize_realtime_voice_control(guild_id=1)
        other.chat_id = "701"
        with pytest.raises(PermissionError, match="linked text"):
            PluginVoiceInvocation(room, other).authorize_realtime_voice_control(guild_id=1)
    finally:
        await attachment.close()


def test_shared_bot_authorizes_against_transport_home_not_runtime_home(room, tmp_path):
    transport = tmp_path / "transport"
    transport.mkdir()

    def transport_check(_source, check):
        token = set_hermes_home_override(transport)
        try:
            return check()
        finally:
            reset_hermes_home_override(token)

    room._under_authorization_profile = transport_check
    room.adapter._is_allowed_user = lambda *_a, **_kw: get_hermes_home() == transport
    factory = VoiceIngressFactory(room, room.source)
    assert factory.admits(42)
