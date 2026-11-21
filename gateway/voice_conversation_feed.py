"""Capability-bound, acknowledged reads of committed conversation additions.

The database is the delivery journal. No notification hook can race persistence,
and a failed provider send leaves the exact batch available for another attempt.
"""
from copy import deepcopy
import logging
import threading

from gateway.response_filters import is_machinery_display_kind

logger = logging.getLogger(__name__)
PAGE_SIZE = 64


def conversation_message(row, session_id):
    role, content = row.get('role'), row.get('content')
    if role not in ('user', 'assistant', 'tool') or not isinstance(content, str) or not content:
        return None
    internal = is_machinery_display_kind(row.get('display_kind')) or role == 'tool'
    return {'message_id': row['id'], 'session_id': session_id,
            'event_id': row.get('message_uid') or str(row['id']),
            'origin': 'internal' if internal else role,
            'role': 'assistant' if internal else role, 'content': content}


class VoiceConversationFeed:
    """One consumer, one pinned session; acknowledgements never skip unseen rows."""

    def __init__(self, attachment, entry):
        self.attachment = attachment
        self.session_id, self.session_key = entry.session_id, entry.session_key
        self._lock = threading.Lock()
        self._pending = None
        self._voice_turn = False
        self._seen = set()
        rows = self._db().get_messages(self.session_id, limit=100, latest=True)
        self._cursor = rows[-1]['id'] if rows else 0
        history, budget = [], 16000
        for row in reversed(rows):
            message = conversation_message(row, self.session_id)
            if message is None:
                continue
            if len(history) >= 20 or len(message['content']) > budget:
                break
            history.append(message)
            budget -= len(message['content'])
        self._snapshot = {'session_id': self.session_id, 'cursor': self._cursor,
                          'messages': list(reversed(history)), 'has_more': False}
        self._seen.update(m['event_id'] for m in history)

    def _db(self):
        store = self.attachment.factory.runner.session_store
        if store.peek_session_id(self.session_key) != self.session_id:
            raise PermissionError('Voice conversation changed; leave and rejoin')
        db = store._db_for_session_id(self.session_id)
        if db is None:
            raise RuntimeError('Voice conversation storage is unavailable')
        if (db.get_compression_tip(self.session_id) or self.session_id) != self.session_id:
            raise PermissionError('Voice conversation was compacted; leave and rejoin')
        return db

    def snapshot(self):
        with self._lock, self.attachment.factory.scope():
            self._db()
            return deepcopy(self._snapshot)

    def read(self):
        with self._lock, self.attachment.factory.scope():
            db = self._db()
            if self._pending is not None:
                return deepcopy(self._pending[0])
            rows = db.get_messages(self.session_id, after_id=self._cursor, limit=PAGE_SIZE)
            owned = {turn.message_id for turn in self.attachment._turns.values()}
            voice_turn, messages, seen = self._voice_turn, [], set(self._seen)
            for row in rows:
                if row['role'] == 'user':
                    voice_turn = (not is_machinery_display_kind(row.get('display_kind'))
                                  and row.get('platform_message_id') in owned)
                message = conversation_message(row, self.session_id)
                if voice_turn or message is None or message['event_id'] in seen:
                    continue
                seen.add(message['event_id'])
                messages.append(message)
                logger.info('Voice context recorded session=%s message=%s origin=%s',
                            self.session_id, row['id'], message['origin'])
            if len(seen) > 8192:
                raise RuntimeError('Voice context capacity reached; leave and rejoin')
            batch = {'session_id': self.session_id,
                     'cursor': rows[-1]['id'] if rows else self._cursor,
                     'messages': messages, 'has_more': len(rows) == PAGE_SIZE}
            self._pending = (batch, voice_turn, seen)
            return deepcopy(batch)

    def acknowledge(self, cursor):
        with self._lock, self.attachment.factory.scope():
            self._db()
            if type(cursor) is not int:
                raise ValueError('Invalid voice context cursor')
            if self._pending is None:
                if cursor == self._cursor:
                    return
                raise ValueError('No such pending voice context batch')
            batch, voice_turn, seen = self._pending
            if cursor != batch['cursor']:
                raise ValueError('Cannot skip an unacknowledged voice context batch')
            self._cursor, self._voice_turn, self._seen = cursor, voice_turn, seen
            self._pending = None
