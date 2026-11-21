"""Automatic audio ownership for a live Realtime attachment's linked text chat."""


def realtime_owns_audio(adapter, chat_id):
    sessions = getattr(adapter, '_continuous_voice_sessions', None)
    if not isinstance(sessions, dict):
        return False
    return any(getattr(session, 'realtime_execution_version', None) == 1
               and getattr(session, 'closed', True) is False
               and str(session.factory.text_channel_id) == str(chat_id)
               for session in tuple(sessions.values()))


def auto_tts_enabled(adapter, chat_id):
    return not realtime_owns_audio(adapter, chat_id) and (
        chat_id in adapter._auto_tts_enabled_chats or (
            chat_id not in adapter._auto_tts_disabled_chats and bool(adapter._auto_tts_default)))
