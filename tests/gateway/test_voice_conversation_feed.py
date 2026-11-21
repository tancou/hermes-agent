"""Committed external turns reach a voice reader without replaying its own tools."""
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from gateway.voice_conversation_feed import VoiceConversationFeed
from hermes_state import SessionDB


@pytest.fixture
def feed(tmp_path):
    db = SessionDB(tmp_path / 'state.db')
    db.create_session('owned', source='discord')
    db.create_session('foreign', source='discord')
    entry = SimpleNamespace(session_id='owned', session_key='bound-key')
    store = SimpleNamespace(_db_for_session_id=lambda _: db,
                            peek_session_id=lambda _: entry.session_id)
    attachment = SimpleNamespace(factory=SimpleNamespace(scope=nullcontext,
        runner=SimpleNamespace(session_store=store)), _turns={})
    value = VoiceConversationFeed(attachment, entry)
    yield value, db, entry, attachment
    db.close()


def test_completed_delegation_and_text_arrive_once_with_original_provenance(feed):
    reader, db, _, attachment = feed
    attachment._turns['voice'] = SimpleNamespace(message_id='realtime-voice:one')
    db.append_message('owned', 'user', 'delegate this', platform_message_id='realtime-voice:one')
    db.append_message('owned', 'tool', 'delegation started', tool_call_id='call-1')
    db.append_message('owned', 'assistant', 'I will let you know.')
    result_id = db.append_message('owned', 'user', 'Found conversation Codex: result-42',
                                  display_kind='internal_notification',
                                  platform_message_id='realtime-voice:one')
    db.append_message('owned', 'assistant', 'The delegated search found result-42.')
    db.append_message('foreign', 'user', 'MUST NOT LEAK')
    db.append_message('owned', 'user', 'A new Discord text message', platform_message_id='discord-8')
    batch = reader.read()
    assert [m['content'] for m in batch['messages']] == [
        'Found conversation Codex: result-42', 'The delegated search found result-42.',
        'A new Discord text message']
    result = batch['messages'][0]
    assert result['origin'] == 'internal' and result['role'] == 'assistant'
    assert result['message_id'] == result_id and result['session_id'] == 'owned'
    assert reader.read() == batch, 'an unacknowledged delivery must remain replayable'
    reader.acknowledge(batch['cursor'])
    reader.acknowledge(batch['cursor'])
    assert reader.read()['messages'] == []


def test_cursor_crosses_pages_inside_a_native_tool_turn_without_duplicate_results(feed):
    reader, db, _, attachment = feed
    attachment._turns['voice'] = SimpleNamespace(message_id='realtime-voice:one')
    db.append_message('owned', 'user', 'start', platform_message_id='realtime-voice:one')
    for _ in range(70):
        db.append_message('owned', 'tool', 'already sent by the tool lane')
    first = reader.read()
    assert first['has_more'] and not first['messages']
    reader.acknowledge(first['cursor'])
    second = reader.read()
    assert not second['messages']
    reader.acknowledge(second['cursor'])
    db.append_message('owned', 'user', 'done', display_kind='async_delegation_complete')
    assert reader.read()['messages'][0]['origin'] == 'internal'


def test_route_replacement_invalidates_feed_instead_of_following_another_session(feed):
    reader, db, entry, _ = feed
    db.append_message('owned', 'user', 'pending result', display_kind='internal_notification')
    pending = reader.read()
    entry.session_id = 'foreign'
    with pytest.raises(PermissionError):
        reader.read()
    with pytest.raises(PermissionError):
        reader.acknowledge(pending['cursor'])


def test_new_connection_snapshot_includes_completion_missed_by_old_connection(feed):
    reader, db, entry, attachment = feed
    db.append_message('owned', 'user', 'completed while disconnected', display_kind='internal_notification')
    missed = reader.read()
    assert reader.read() == missed
    replacement = VoiceConversationFeed(attachment, entry)
    snapshot = replacement.snapshot()
    assert snapshot['messages'][0]['content'] == 'completed while disconnected'
    assert snapshot['messages'][0]['role'] == 'assistant'
    assert replacement.read()['messages'] == []
