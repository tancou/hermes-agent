"""DISCORD_ALLOWED_VOICE_CHANNELS applies before connecting, before moving, and when the bot is dragged."""
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.config import PlatformConfig
from plugins.platforms.discord.adapter import DiscordAdapter


def _adapter(monkeypatch, allowed=None):
    monkeypatch.delenv("DISCORD_ALLOWED_VOICE_CHANNELS", raising=False)
    monkeypatch.setattr("plugins.platforms.discord.adapter.DISCORD_AVAILABLE", True)
    extra = {} if allowed is None else {"allowed_voice_channels": allowed}
    adapter = DiscordAdapter(PlatformConfig(enabled=True, token="fixture", extra=extra))
    adapter._client = MagicMock()
    return adapter


@pytest.mark.parametrize("allowed,expected", [
    (None, True), ("", True), ("2", True), ("3, 2", True), ([3, 2], True), ("*", True), ("3", False), (["3"], False),
])
def test_voice_channel_allowlist(monkeypatch, allowed, expected):
    assert _adapter(monkeypatch, allowed)._voice_channel_allowed(SimpleNamespace(id=2)) is expected


def test_env_overrides_config(monkeypatch):
    adapter = _adapter(monkeypatch, "2")
    monkeypatch.setenv("DISCORD_ALLOWED_VOICE_CHANNELS", "3")
    assert adapter._voice_channel_allowed(SimpleNamespace(id=2)) is False


@pytest.mark.asyncio
@pytest.mark.parametrize("existing", [False, True])
async def test_join_refuses_unlisted_channel_before_connect_or_move(monkeypatch, existing):
    adapter = _adapter(monkeypatch, "2")
    connection = SimpleNamespace(is_connected=lambda: True, move_to=AsyncMock())
    if existing:
        adapter._voice_clients[1] = connection
    channel = SimpleNamespace(id=3, guild=SimpleNamespace(id=1), connect=AsyncMock())
    assert await adapter.join_voice_channel(channel) is False
    channel.connect.assert_not_awaited()
    connection.move_to.assert_not_awaited()
