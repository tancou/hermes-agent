"""A room member cannot inherit the joiner's pinned runtime routing identity."""
import asyncio
from contextlib import nullcontext
from types import SimpleNamespace as NS
import weakref

import pytest

from gateway.config import Platform
from gateway.plugin_voice import VoiceIngressFactory
from gateway.profile_routing import match_profile_route, parse_profile_routes
from gateway.realtime_execution import RealtimeExecutionAttachment, VoiceTurn
from gateway.session import SessionSource
from gateway.session_identity import canonical_identity, identity_of
from hermes_constants import get_process_hermes_home


def test_realtime_members_are_rerouted_and_foreign_profiles_refused(monkeypatch):
    class Adapter:
        pass
    adapter = Adapter()
    home = get_process_hermes_home().resolve()
    for profile in ('devops', 'guest'):
        (home / profile).mkdir(exist_ok=True)
    routes = parse_profile_routes([
        {'platform': 'discord', 'user_id': str(user), 'profile': profile}
        for user, profile in ((42, 'devops'), (43, 'devops'), (99, 'guest'))])
    channel = NS(id=90, guild=NS(id=1), members=[NS(id=n, bot=False) for n in (42, 43, 99)])
    adapter._voice_clients = {1: NS(channel=channel, is_connected=lambda: True)}
    adapter._voice_text_channels = {1: 700}
    adapter._voice_input_callback = lambda **_: None
    adapter._is_allowed_user = lambda *_, **__: True
    runner = NS(
        config=NS(multiplex_profiles=True, profile_routes=routes,
                  _runtime_profile_homes=(('default', home), ('devops', home / 'devops'),
                                          ('guest', home / 'guest'))),
        _primary_profile_name='default', adapters={Platform.DISCORD: adapter},
        _native_transport_homes={None: home},
        _transport_owner=lambda _: (adapter, None),
        _owning_profile=lambda *_: (True, None),
        _profile_name_for_source=lambda s: match_profile_route(
            routes, 'discord', user_id=s.user_id).profile,
        _resolve_profile_home_for_source=lambda s: home / s.profile,
        _profile_scope_for_source=lambda _: nullcontext(),
        _under_authorization_profile=lambda _, check: check(),
        _is_user_authorized_for_source=lambda _: True,
        _intake_adapter_for=lambda _: adapter)
    source = SessionSource(Platform.DISCORD, '700', user_id='42', user_name='joiner', guild_id='1')
    source._transport_adapter_ref = weakref.ref(adapter)
    canonical_identity(source, runner=runner)
    monkeypatch.setattr('hermes_cli.config_effective.load_user_config_effective',
                        lambda **_: {'discord': {'voice_operator_only': False}})
    factory = VoiceIngressFactory(runner, source)

    async def run():
        attachment = RealtimeExecutionAttachment(factory)
        try:
            assert attachment.admits_speaker(43)
            assert not attachment.admits_speaker(99)
            with pytest.raises(PermissionError, match='outside this attachment profile'):
                attachment.check(99)
            turn = VoiceTurn(attachment, 43, 'synthetic', 'input-43')
            assert turn.source.user_id == '43'
            assert turn.source.user_name != 'joiner'
            assert turn.source._transport_adapter_ref() is adapter
            assert identity_of(turn.source) is not identity_of(factory.source)
            assert identity_of(turn.source).runtime_profile == 'devops'
        finally:
            attachment.close()
    asyncio.run(run())
