"""Final transcript arrival must not advance the authenticated input timeline."""
import asyncio
from types import SimpleNamespace

import pytest

from gateway.realtime_execution import RealtimeExecutionAttachment
from gateway.session import SessionSource
from gateway.config import Platform


@pytest.mark.asyncio
async def test_late_final_does_not_end_a_more_recent_tool_turn(monkeypatch):
    factory = SimpleNamespace(
        operator_user_id=42, source=SessionSource(Platform.DISCORD, '700', user_id='42'),
        admits=lambda _: True, release=lambda _: None,
        realtime_source=lambda _: SessionSource(Platform.DISCORD, '700', user_id='42'))
    attachment = RealtimeExecutionAttachment(factory)
    # Only the worker transport is controlled; the host turn state is real.
    async def pending(_turn):
        await asyncio.Event().wait()
    monkeypatch.setattr('gateway.realtime_execution.VoiceTurn.run', pending)
    try:
        attachment.note_input(42, 'recent B', input_item_id='B')
        recent = attachment._turn(42, 'B')
        attachment.note_input(42, 'late A', input_item_id='A')
        assert not recent.ended
        assert attachment._inputs[(42, 'A')] == 'late A'
        assert attachment._inputs[(42, 'B')] == 'recent B'
    finally:
        attachment.close()
        for task in tuple(attachment._tasks):
            task.cancel()
        await asyncio.gather(*attachment._tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_vad_progression_is_single_use_and_finales_do_not_reorder_it(monkeypatch):
    factory = SimpleNamespace(
        operator_user_id=42, source=SessionSource(Platform.DISCORD, '700', user_id='42'),
        admits=lambda _: True, release=lambda _: None,
        realtime_source=lambda _: SessionSource(Platform.DISCORD, '700', user_id='42'))
    attachment = RealtimeExecutionAttachment(factory)
    async def pending(_turn):
        await asyncio.Event().wait()
    monkeypatch.setattr('gateway.realtime_execution.VoiceTurn.run', pending)
    try:
        attachment.note_input_started(42, input_item_id='A')
        old = attachment._turn(42, 'A')
        attachment.note_input_started(42, input_item_id='B')
        assert old.ended
        recent = attachment._turn(42, 'B')
        attachment.note_input(42, 'B first', input_item_id='B')
        attachment.note_input(42, 'A late', input_item_id='A')
        attachment.note_input_started(42, input_item_id='A')
        assert not recent.ended
        assert attachment._active_inputs[42] == (42, 'B')
    finally:
        attachment.close()
        for task in tuple(attachment._tasks):
            task.cancel()
        await asyncio.gather(*attachment._tasks, return_exceptions=True)
