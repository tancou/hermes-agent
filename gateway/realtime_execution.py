"""Discord Realtime model responses executed by the canonical gateway turn.

Only live command capabilities create these requests. Durable admissions retain
their opaque message id; after a restart an expired capability refuses execution
instead of replaying a provider's old tool calls through a different model.
"""
import asyncio
from copy import deepcopy
import json
import logging
import queue
import threading
import time
import uuid

from gateway.platforms.event import MessageEvent

logger = logging.getLogger(__name__)
_PREFIX = 'realtime-voice:'
_requests = {}


def request_for(message_id):
    return _requests.get(message_id)


def run_voice_conversation(agent, message, kwargs, *, message_id, source):
    """Called inside TurnRunner's ordinary approval and session contexts."""
    if not str(message_id or '').startswith(_PREFIX):
        return agent.run_conversation(message, **kwargs)
    request = request_for(message_id)
    if request is None:
        raise RuntimeError('Realtime voice attachment expired; rejoin before continuing')
    request.check_source(source)
    if agent.api_mode != "chat_completions":
        raise RuntimeError("Canonical Realtime execution requires a chat_completions host profile")
    from agent.external_completion import bind_completion
    with bind_completion(agent, request.complete):
        try:
            result = agent.run_conversation(message, **kwargs)
            request.result = result
            return result
        finally:
            request.finished.set()


class VoiceTurn:
    """One canonical user turn, with provider replies arriving independently of audio."""

    def __init__(self, attachment, user_id, text, input_item_id):
        self.attachment, self.user_id, self.text = attachment, user_id, text
        self.input_item_id = input_item_id
        self.input_ready = asyncio.Event()
        if text:
            self.input_ready.set()
        self.message_id = _PREFIX + uuid.uuid4().hex
        self.source = attachment.check(user_id)
        self.incoming = queue.Queue(maxsize=32)
        self.finished = threading.Event()
        self.result = None
        self.pending = None
        self.ended = False
        self.task = None

    def check_source(self, source):
        expected = self.source
        if any(getattr(source, key) != getattr(expected, key) for key in
               ('platform', 'chat_id', 'user_id', 'profile', 'thread_id')):
            raise PermissionError('Realtime request belongs to another conversation')
        self.attachment.check(self.user_id)

    def _settle(self, future, value):
        def resolve():
            if not future.done():
                future.set_result(value)
        self.attachment.loop.call_soon_threadsafe(resolve)

    def complete(self, agent, payload):
        from openai.types.chat import ChatCompletion
        self.attachment.check(self.user_id)
        if self.pending is not None:
            calls, future = self.pending
            messages = payload.get('messages', [])
            outputs = {m.get('tool_call_id'): m.get('content') for m in messages if m.get('role') == 'tool'}
            values = [{'call_id': call['call_id'], 'output': str(outputs.get(call['call_id'],
                       'Hermes refused or interrupted this tool; no successful result was recorded.'))}
                      for call in calls]
            self._settle(future, values)
            self.pending = None
        deadline = time.monotonic() + 180
        while True:
            if agent._interrupt_requested or self.attachment.closed or time.monotonic() > deadline:
                raise InterruptedError('Realtime voice turn ended')
            try:
                kind, value = self.incoming.get(timeout=0.1)
                break
            except queue.Empty:
                continue
        if kind == 'tools':
            calls, future = value
            self.pending = (calls, future)
            message = {'role': 'assistant', 'content': None, 'tool_calls': [
                {'id': c['call_id'], 'type': 'function', 'function': {
                    'name': c['tool_name'], 'arguments': json.dumps(c['arguments'])}} for c in calls]}
            reason = 'tool_calls'
        else:
            message, reason = {'role': 'assistant', 'content': value or '[Voice response interrupted.]'}, 'stop'
        return ChatCompletion(id='voice-' + uuid.uuid4().hex, created=int(time.time()),
                              model='external-realtime', object='chat.completion',
                              choices=[{'index': 0, 'message': message, 'finish_reason': reason}],
                              usage={'prompt_tokens': 0, 'completion_tokens': 0, 'total_tokens': 0})

    def finish(self, text=''):
        if not self.ended:
            self.ended = True
            self.incoming.put_nowait(('final', text))

    async def run(self):
        _requests[self.message_id] = self
        try:
            await asyncio.wait_for(self.input_ready.wait(), 30)
            self.attachment.check(self.user_id)
            with self.attachment.factory.scope():
                event = MessageEvent(text=self.text or '[Voice input]', source=self.source,
                                     message_id=self.message_id, allow_gateway_control=False,
                                     reply_expected=False)
                # Re-enter the receiving adapter callback: it establishes fresh native
                # provenance for THIS event, including multiplexed profile ownership.
                await self.attachment.factory.adapter._message_handler(event)
                if self.result is None:
                    raise RuntimeError('Hermes did not execute the admitted voice turn')
        finally:
            self.finished.set()
            _requests.pop(self.message_id, None)
            if self.pending is not None:
                calls, future = self.pending
                self._settle(future, [{'call_id': c['call_id'], 'output':
                    'Hermes stopped this turn; inspect its history before retrying.'} for c in calls])


class RealtimeExecutionAttachment:
    """Opaque permits and native turn execution for a host-authenticated voice room."""

    realtime_execution_version = 1

    def __init__(self, factory):
        self.factory = factory
        self.operator_user_id = factory.operator_user_id
        self.loop = asyncio.get_running_loop()
        self.closed = False
        self.trusted_room = False
        self.approvals_disabled = False
        self._definitions = []
        self._history = []
        self._permits = {}
        self._seen = set()
        self._turns = {}
        self._inputs = {}
        self._active_inputs = {}
        self._started_inputs = set()
        self._tasks = set()

    async def prepare(self):
        self.check(self.operator_user_id)
        def discover():
            import model_tools
            from hermes_cli.config_effective import load_user_config_effective
            with self.factory.scope():
                trusted = load_user_config_effective(fail_closed=True).get("discord", {}).get("voice_trusted_room", False)
                if type(trusted) is not bool:
                    raise ValueError("discord.voice_trusted_room must be a boolean")
                self.trusted_room = trusted
                from tools.approval_context import _get_approval_mode
                self.approvals_disabled = _get_approval_mode() == "off"
                display = self.factory.runner._run_agent_display_settings(self.factory.source)
                definitions = model_tools.get_tool_definitions(enabled_toolsets=display.enabled_toolsets,
                    disabled_toolsets=display.disabled_toolsets, quiet_mode=True)
                store = self.factory.runner.session_store
                entry = store.get_or_create_session(self.factory.source, touch_activity=False)
                history, budget = [], 16000
                for row in reversed(store.load_transcript(entry.session_id)):
                    content = row.get('content')
                    if row.get('role') not in ('user', 'assistant') or not isinstance(content, str) or not content:
                        continue
                    if len(history) >= 20 or len(content) > budget:
                        break
                    history.append({'role': row['role'], 'content': content})
                    budget -= len(content)
                self._history = list(reversed(history))
                return [dict(type='function', **d['function']) for d in definitions]
        self._definitions = await asyncio.to_thread(discover)
        slots = getattr(self.factory.adapter, '_continuous_voice_sessions', None)
        if slots is None:
            slots = self.factory.adapter._continuous_voice_sessions = {}
        if self.factory.guild_id in slots:
            raise RuntimeError('This voice channel already has a canonical attachment')
        slots[self.factory.guild_id] = self
        return self

    def check(self, user_id):
        if self.closed or not self.factory.admits(user_id):
            raise PermissionError('Voice speaker is no longer authorized in this room')
        return self.factory.realtime_source(user_id)

    def initial_history(self):
        self.check(self.operator_user_id)
        return deepcopy(self._history)

    def tool_definitions(self):
        self.check(self.operator_user_id)
        return deepcopy(self._definitions)

    def admits_speaker(self, user_id):
        try:
            self.check(user_id)
        except PermissionError:
            return False
        return True

    def note_input_started(self, user_id, *, input_item_id):
        """Advance on authenticated VAD/commit order, independently of final text."""
        self.check(user_id)
        if not isinstance(input_item_id, str) or not 0 < len(input_item_id) <= 512:
            raise PermissionError('Missing authenticated voice input identity')
        key = (user_id, input_item_id)
        if key in self._started_inputs:
            return
        if len(self._started_inputs) >= 4096:
            raise RuntimeError('Voice history capacity reached; rejoin')
        self._started_inputs.add(key)
        previous = self._turns.get(self._active_inputs.get(user_id))
        if previous is not None and not previous.ended:
            previous.finish('[Voice response interrupted by new speech.]')
        self._active_inputs[user_id] = key

    def note_input(self, user_id, text, *, input_item_id=None):
        self.check(user_id)
        if not isinstance(text, str) or len(text) > 32000:
            raise ValueError('Invalid voice transcript')
        if not isinstance(input_item_id, str) or not 0 < len(input_item_id) <= 512:
            raise PermissionError("Missing authenticated voice input identity")
        key = (user_id, input_item_id)
        self._inputs[key] = text
        turn = self._turns.get(key)
        if turn is not None:
            turn.text = text
            turn.input_ready.set()

    def _turn(self, user_id, input_item_id):
        if not isinstance(input_item_id, str) or not 0 < len(input_item_id) <= 512:
            raise PermissionError('Missing authenticated voice input identity')
        key = (user_id, input_item_id)
        turn = self._turns.get(key)
        if turn is None:
            if len(self._turns) >= 4096:
                raise RuntimeError('Voice history capacity reached; rejoin')
            turn = self._turns[key] = VoiceTurn(self, user_id, self._inputs.get(key, ''), input_item_id)
            turn.task = self.loop.create_task(turn.run())
            self._tasks.add(turn.task)
            turn.task.add_done_callback(self._done)
        return turn

    def _done(self, task):
        self._tasks.discard(task)
        if not task.cancelled() and task.exception() is not None:
            logger.error('Canonical Realtime turn failed: %s', type(task.exception()).__name__)

    def note_response(self, response_id, user_id, text, *, has_tools=False, input_item_id=None):
        self.check(user_id)
        if has_tools:
            return
        turn = self._turn(user_id, input_item_id)
        turn.finish(text)

    def mint_tool_call_permit(self, *, response_id, item_id, call_id, batch_id,
                              tool_name, arguments, speaker_user_id=None, input_item_id=None):
        user_id = speaker_user_id if speaker_user_id is not None else self.operator_user_id
        self.check(user_id)
        if call_id in self._seen or len(self._seen) >= 4096:
            raise PermissionError('Repeated tool call or exhausted voice session')
        if tool_name not in {d['name'] for d in self._definitions}:
            raise PermissionError('Tool is not in this profile voice surface')
        if not all(isinstance(v, str) and 0 < len(v) <= 512 for v in (response_id, item_id, call_id, batch_id)):
            raise ValueError('Invalid tool call identity')
        if type(arguments) is not dict or len(json.dumps(arguments)) > 128000:
            raise ValueError('Invalid tool arguments')
        self._seen.add(call_id)
        permit = object()
        self._permits[permit] = dict(response_id=response_id, call_id=call_id, batch_id=batch_id,
                                    tool_name=tool_name, arguments=deepcopy(arguments), user_id=user_id, input_item_id=input_item_id)
        return permit

    async def execute_tool_batch(self, permits):
        calls = [self._permits.pop(p) for p in permits]
        if not calls or len(calls) > 32 or len({(c['batch_id'], c['user_id'], c['input_item_id']) for c in calls}) != 1:
            raise PermissionError('Invalid canonical tool batch')
        user_id = calls[0]['user_id']
        self.check(user_id)
        turn = self._turn(user_id, calls[0]["input_item_id"])
        if turn.ended:
            raise PermissionError("This voice input turn has already ended")
        future = self.loop.create_future()
        turn.incoming.put_nowait(('tools', (calls, future)))
        done, _ = await asyncio.wait((future, turn.task), return_when=asyncio.FIRST_COMPLETED)
        if future in done:
            return future.result()
        await turn.task
        raise RuntimeError('Canonical voice turn ended before returning tool results')

    def close(self):
        for turn in self._turns.values():
            turn.finish('[Voice session closed.]')
        self.closed = True
        self._permits.clear()
        self.factory.release(self)
