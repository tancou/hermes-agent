"""Real gateway/agent/tool loop, with only Discord and model output synthetic."""
import asyncio
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace
import weakref


async def main():
    from gateway.config import Platform, PlatformConfig
    from gateway.platforms.base import BasePlatformAdapter, SendResult
    from gateway.run import GatewayRunner
    from gateway.session import SessionSource
    from gateway.session_authority import initialize_session_authority
    from gateway.plugin_voice import PluginVoiceInvocation
    from hermes_state_runtime import list_session_admissions

    class Adapter(BasePlatformAdapter):
        def __init__(self):
            super().__init__(PlatformConfig(enabled=True, token='fixture'), Platform.DISCORD)
            channel = SimpleNamespace(id=90, guild=SimpleNamespace(id=1), members=[SimpleNamespace(id=42, bot=False)])
            self._voice_clients = {1: SimpleNamespace(channel=channel, is_connected=lambda: True)}
            self._voice_text_channels = {1: 700}
            self._voice_input_callback = lambda **_: None
            self._is_allowed_user = lambda user, **_: user == '42'
            self.sent = []
            self.acks = []
            self.voice_mixer_active = lambda _: True

        async def play_ack_in_voice(self, guild_id):
            self.acks.append(guild_id)

        async def connect(self, **kwargs):
            return True

        async def disconnect(self):
            pass

        async def send(self, chat_id, content, **kwargs):
            self.sent.append(content)
            return SendResult(success=True, message_id='1')

        async def send_typing(self, *args, **kwargs):
            pass

        async def get_chat_info(self, chat_id):
            return {'id': chat_id}

    root = Path(os.environ['HERMES_HOME'])
    profile = sys.argv[1] if len(sys.argv) > 1 else 'default'
    trusted = len(sys.argv) > 2 and sys.argv[2] == 'trusted'
    runner = GatewayRunner()
    adapter = Adapter()
    if profile == 'default':
        await initialize_session_authority(runner, profile_id='default', instance_id='duplex-test')
        runner.adapters[Platform.DISCORD] = adapter
        handler = runner._make_default_profile_message_handler()
        home = root
    else:
        from gateway.run_runtime import initialize_gateway_runtime
        from gateway.runtime_ownership import process_ownership
        home = root / 'profiles' / profile
        home.mkdir(parents=True)
        policy = '\ndiscord:\n  voice_trusted_room: true\napprovals:\n  mode: off\n' if trusted else ''
        (home / 'config.yaml').write_text((root / 'config.yaml').read_text() + policy)
        (home / '.env').write_text('OPENAI_API_KEY=offline-only\nOPENAI_BASE_URL=http://127.0.0.1:9/v1\n'
                                  'DISCORD_ALLOWED_USERS=42\nDISCORD_ALLOW_ALL_USERS=true\n')
        runner.config.multiplex_profiles = True
        runner.config._runtime_profile_homes = (('default', root), (profile, home))
        process_ownership.reserve([root, home])
        await initialize_gateway_runtime(runner)
        runner.adapters[Platform.DISCORD] = Adapter()
        runner._profile_adapters = {profile: {Platform.DISCORD: adapter}}
        handler = runner._make_profile_message_handler(profile)
    adapter.set_message_handler(handler)
    source = SessionSource(Platform.DISCORD, '700', chat_type='dm', user_id='42', guild_id='1',
                           profile=None if profile == 'default' else profile)
    source._transport_adapter_ref = weakref.ref(adapter)
    source._authorization_profile_home = home
    invocation = PluginVoiceInvocation(runner, source)
    with runner._profile_scope_for_source(source):
        attachment = await invocation.capture_realtime_execution_attachment()
        assert attachment.trusted_room is trusted
        assert attachment.approvals_disabled is trusted
        assert any(t['name'] == 'terminal' for t in attachment.tool_definitions())
        permit = attachment.mint_tool_call_permit(response_id='response-1', item_id='item-1',
            call_id='call-1', batch_id='batch-1', tool_name='terminal', speaker_user_id=42, input_item_id='fixture-input',
            arguments={'command': 'printf native-duplex'})
        executing = asyncio.create_task(attachment.execute_tool_batch((permit,)))
        from tests.waiting import async_wait_until
        await async_wait_until(lambda: (42, 'fixture-input') in attachment._turns, timeout=5)
        assert not executing.done(), 'tool must wait for its exact final input transcript'
        attachment.note_input(42, 'Run printf native-duplex, then tell me its output.', input_item_id='fixture-input')
        results = await asyncio.wait_for(executing, 45)
        assert 'native-duplex' in results[0]['output'], results
        turn = attachment._turns[(42, 'fixture-input')]
        assert not turn.task.done(), 'audio continuation must remain independent of the canonical worker'
        # A real plugin-requested approval still traverses the gateway gate.
        from hermes_cli.plugins import PluginContext, PluginManifest, get_plugin_manager
        from tools import approval
        context = PluginContext(PluginManifest(name='voice-test-policy', source='user'), get_plugin_manager())
        context.register_hook('pre_tool_call', lambda **_: {'action': 'approve', 'message': 'fixture approval'})
        blocked_permit = attachment.mint_tool_call_permit(response_id='response-approval', item_id='item-approval',
            call_id='call-approval', batch_id='batch-approval', tool_name='terminal', speaker_user_id=42,
            input_item_id='fixture-input', arguments={'command': 'printf forbidden > denied-marker'})
        blocked = asyncio.create_task(attachment.execute_tool_batch((blocked_permit,)))
        if trusted:
            allowed = await asyncio.wait_for(blocked, 15)
            assert 'denied' not in allowed[0]['output'].lower(), allowed
            assert Path(os.environ['HERMES_HOME'], 'denied-marker').read_text() == 'forbidden'
            assert approval.pending_gateway_approval_count() == 0
        else:
            for _ in range(300):
                with approval._lock:
                    keys = [key for key, entries in approval._gateway_queues.items() if entries]
                if keys:
                    break
                await asyncio.sleep(0.05)
            else:
                raise AssertionError('Native approval was not raised')
            assert len(keys) == 1
            assert approval.list_gateway_approval_authority(keys[0])[0]['requester_id'] == '42'
            approval.resolve_gateway_approval(keys[0], 'deny')
            denied = await asyncio.wait_for(blocked, 15)
            assert 'den' in denied[0]['output'].lower(), denied
            assert not Path(os.environ['HERMES_HOME'], 'denied-marker').exists()
        attachment.note_response('response-2', 42, 'The output is native-duplex.', input_item_id='fixture-input')
        await asyncio.wait_for(turn.task, 15)
        assert turn.result['completed'], turn.result
        messages = turn.result['messages']
        assert any(m.get('role') == 'tool' and m.get('tool_call_id') == 'call-1' for m in messages), messages
        assert messages[-1]['content'] == 'The output is native-duplex.', messages[-1]
        assert adapter.acks == [], 'Native TTS must not overlap Realtime audio'
        from gateway.session_authorities import active_authority
        entry = runner.session_store.get_or_create_session(source)
        admissions = list_session_admissions(active_authority(runner).db, session_id=entry.session_id, pending_only=False)
        assert len(admissions) == 1 and admissions[0]['status'] == 'terminal', admissions
        try:
            await attachment.execute_tool_batch((permit,))
        except (KeyError, PermissionError):
            pass
        else:
            raise AssertionError('Repeated permit executed')
        attachment.close()
        resumed = await invocation.capture_realtime_execution_attachment()
        assert resumed.initial_history()[-1] == {'role': 'assistant', 'content': 'The output is native-duplex.'}
        resumed.close()
        Path(os.environ['HERMES_HOME'], 'duplex-receipt.json').write_text(json.dumps({
            'tools': results, 'completed': True, 'durable_turns': len(admissions),
            'history_roles': [m['role'] for m in messages], 'sent': adapter.sent,
        }))

    if trusted:
        from tools.approval_context import _get_approval_mode
        assert _get_approval_mode() != 'off', 'DevOps approval mode must not leak to launch profile'


if __name__ == '__main__':
    os.environ.update(OPENAI_API_KEY='offline-only', OPENAI_BASE_URL='http://127.0.0.1:9/v1',
                      DISCORD_ALLOWED_USERS='42', DISCORD_ALLOW_ALL_USERS='true')
    Path(os.environ['HERMES_HOME'], 'config.yaml').write_text(
        'model:\n  default: offline-model\n  provider: custom\n  base_url: http://127.0.0.1:9/v1\n'
        'auxiliary:\n  title_generation:\n    enabled: false\n'
        'memory:\n  memory_enabled: false\n  user_profile_enabled: false\n'
        'toolsets:\n  discord: [terminal]\n'
        f'terminal:\n  cwd: {os.environ["HERMES_HOME"]}\n'
        'display:\n  tool_progress: off\n  thinking_progress: false\n')
    asyncio.run(main())
