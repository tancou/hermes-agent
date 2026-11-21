"""Realtime owns automatic audio only on its live adapter and linked text chat."""
from types import SimpleNamespace

from gateway.voice_output_policy import auto_tts_enabled


def test_live_realtime_suppresses_auto_tts_and_restores_original_chat_settings():
    session = SimpleNamespace(realtime_execution_version=1, closed=False,
                              factory=SimpleNamespace(text_channel_id=700))
    adapter = SimpleNamespace(_auto_tts_default=True, _auto_tts_enabled_chats={'700'},
                              _auto_tts_disabled_chats={'900'}, _continuous_voice_sessions={1: session})
    assert not auto_tts_enabled(adapter, '700')
    assert auto_tts_enabled(adapter, '800')
    assert not auto_tts_enabled(adapter, '900')
    other_bot = SimpleNamespace(_auto_tts_default=True, _auto_tts_enabled_chats=set(),
                                _auto_tts_disabled_chats=set())
    assert auto_tts_enabled(other_bot, '700')
    session.closed = True
    assert auto_tts_enabled(adapter, '700')
    session.closed = False
    del adapter._continuous_voice_sessions[1]
    assert auto_tts_enabled(adapter, '700')
    # Continuous transcription still relies on native TTS.
    session.realtime_execution_version = None
    adapter._continuous_voice_sessions[1] = session
    assert auto_tts_enabled(adapter, '700')
