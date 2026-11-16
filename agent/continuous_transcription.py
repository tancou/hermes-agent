"""Optional continuous STT contract; providers never own tools or speaker identity."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TranscriptionEvent:
    """One provider event. Only final transcripts become canonical user messages.

    ``item_id`` must remain stable across partial/final deliveries and reconnects
    within this session. The host attaches the transport-authenticated speaker.
    """

    kind: str
    item_id: str = ""
    text: str = ""
    final: bool = False

    def __post_init__(self):
        if self.kind not in {"ready", "speech_started", "transcript"}:
            raise ValueError("Unknown transcription event")
        if self.kind != "ready" and (not isinstance(self.item_id, str)
                                     or not self.item_id or len(self.item_id) > 512):
            raise ValueError("A transcription event needs a bounded item id")
        if not isinstance(self.text, str) or len(self.text) > 32_000 or type(self.final) is not bool:
            raise ValueError("Invalid transcription payload")


class ContinuousTranscriptionSession(ABC):
    """One speaker, 24 kHz mono PCM16LE, many utterances, one owning asyncio loop.

    The provider emits ``ready`` after its configuration is acknowledged. It
    raises on transport/protocol failure and closes idempotently. It must disable
    automatic assistant responses and tools. No provider event conveys authority.
    """

    @abstractmethod
    async def send_audio(self, pcm: bytes) -> None:
        """Send one bounded PCM chunk; backpressure belongs to the caller."""

    @abstractmethod
    def events(self) -> AsyncIterator[TranscriptionEvent]:
        """Yield ordered input events; unexpected EOF ends the attachment."""

    @abstractmethod
    async def close(self) -> None:
        """Release the transport, including after cancellation."""
