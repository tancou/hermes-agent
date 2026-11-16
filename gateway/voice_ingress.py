"""Host-owned continuous voice ingress into the existing Discord turn pipeline."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from agent.continuous_transcription import ContinuousTranscriptionSession, TranscriptionEvent

MAX_SPEAKERS = 8
MAX_ITEMS = 4096
IO_TIMEOUT = 30


@dataclass(frozen=True)
class VoiceLifecycle:
    lifecycle: str


class VoiceIngressAttachment:
    """Bound to one voice connection/text binding; one STT socket per speaker.

    No merged-speaker transcription is ever attributed to the command's author.
    The gateway's ordinary voice callback owns routing, authorization, tools,
    approvals, persistence, memory and TTS. Audio admission has no tool authority.
    """

    def __init__(self, factory, provider):
        self.factory, self.provider = factory, provider
        self.operator_user_id = factory.operator_user_id
        self.operator_only = factory.operator_only
        self.lifecycle_events = ()
        self._sessions = {}
        self._readers = set()
        self._ready = {}
        self._seen = {}
        self._pending = asyncio.Queue(maxsize=32)
        self._failure = None
        self._closed = False
        self._delivery = asyncio.create_task(self._deliver())

    def check(self):
        if self._failure is not None:
            raise RuntimeError("Continuous voice session failed") from self._failure
        if self._closed:
            raise RuntimeError("Continuous voice session is closed")
        self.factory.check_binding()

    async def start(self):
        try:
            await self._speaker(self.operator_user_id)
        except BaseException:
            await self.close()
            raise
        self.lifecycle_events = (VoiceLifecycle("listening"),)

    async def _speaker(self, user_id):
        with self.factory.scope():
            return await self._open_speaker(user_id)

    async def _open_speaker(self, user_id):
        if user_id in self._sessions:
            return self._sessions[user_id]
        if len(self._sessions) >= MAX_SPEAKERS:
            raise RuntimeError("Continuous voice speaker capacity reached; rejoin to reset")
        session = await asyncio.wait_for(self.provider.open_continuous_session(), IO_TIMEOUT)
        if not isinstance(session, ContinuousTranscriptionSession):
            await session.close()
            raise TypeError("Provider returned an incompatible continuous session")
        self._sessions[user_id] = session
        self._ready[user_id] = asyncio.Event()
        reader = asyncio.create_task(self._read(user_id, session))
        self._readers.add(reader)
        await asyncio.wait_for(self._ready[user_id].wait(), IO_TIMEOUT)
        self.check()
        return session

    async def _send(self, session, pcm):
        with self.factory.scope():
            await asyncio.wait_for(session.send_audio(pcm), IO_TIMEOUT)

    async def _read(self, user_id, session):
        try:
            async for event in session.events():
                self.check()
                if not isinstance(event, TranscriptionEvent):
                    raise TypeError("Invalid continuous transcription event")
                if event.kind == "ready":
                    self._ready[user_id].set()
                elif event.kind == "transcript" and event.final:
                    if not self._ready[user_id].is_set():
                        raise RuntimeError("Transcript received before provider readiness")
                    key = (user_id, event.item_id)
                    if key in self._seen:
                        if self._seen[key] != event.text:
                            raise RuntimeError("Conflicting final transcript")
                        continue
                    if len(self._seen) >= MAX_ITEMS:
                        raise RuntimeError("Continuous voice transcript capacity reached; rejoin to reset")
                    self._seen[key] = event.text
                    if event.text.strip():
                        self._pending.put_nowait((user_id, event.text.strip()))
            if not self._closed:
                raise RuntimeError("Continuous transcription stream ended")
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # health: allow BLE001 -- retained and re-raised by check(), never discarded
            self._failure = exc
            self._ready[user_id].set()

    async def _deliver(self):
        try:
            while True:
                user_id, text = await self._pending.get()
                self.check()
                if self.factory.admits(user_id):
                    from tools.voice_mode_transcript import is_whisper_hallucination
                    if not is_whisper_hallucination(text):
                        await self.factory.deliver(user_id, text)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # health: allow BLE001 -- retained and re-raised by check(), never discarded
            self._failure = exc

    async def feed_audio(self, pcm, *, speaker_user_id, mime_type="audio/pcm"):
        self.check()
        self._validate_pcm(pcm)
        if mime_type != "audio/pcm":
            raise ValueError("Unsupported voice audio type")
        if not self.factory.admits(speaker_user_id):
            await self.feed_synthesized_silence(bytes(len(pcm)))
            return "ignored"
        session = await self._speaker(speaker_user_id)
        await self._send(session, pcm)
        # A different speaker talking must also let the previous speaker's VAD
        # close its turn. Never send that person's speech into another socket.
        for user_id, other in tuple(self._sessions.items()):
            if user_id != speaker_user_id:
                await self._send(other, bytes(len(pcm)))
        self.factory.adapter._reset_voice_timeout(self.factory.guild_id)
        return "accepted"

    async def feed_synthesized_silence(self, pcm):
        self.check()
        self._validate_pcm(pcm)
        if any(pcm):
            raise ValueError("Unattributed audio must be silence")
        for session in tuple(self._sessions.values()):
            await self._send(session, pcm)
        return "accepted"

    @staticmethod
    def _validate_pcm(pcm):
        if type(pcm) is not bytes or not pcm or len(pcm) % 2 or len(pcm) > 48_000:
            raise ValueError("Expected at most one second of PCM16LE")

    async def close(self):
        if self._closed:
            return
        self._closed = True
        self.lifecycle_events = (VoiceLifecycle("closed"),)
        tasks = (*self._readers, self._delivery)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        try:
            with self.factory.scope():
                await asyncio.gather(*(asyncio.wait_for(s.close(), IO_TIMEOUT)
                                       for s in self._sessions.values()), return_exceptions=True)
        finally:
            self.factory.release(self)
