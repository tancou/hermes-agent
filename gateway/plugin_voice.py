"""Live command capability for canonical voice ingress, scoped by the gateway."""

from __future__ import annotations

from gateway.config import Platform
from gateway.session_identity import replace_source


class PluginVoiceInvocation:
    """Short-lived command context. Factories captured during it may outlive it."""

    transcription_ingress_version = 1
    realtime_execution_version = 1

    def __init__(self, runner, source):
        self.runner, self.source = runner, replace_source(source)
        self.active = True

    @property
    def realtime_voice_default(self):
        from hermes_cli.config_effective import load_user_config_effective
        with self.runner._profile_scope_for_source(self.source):
            engine = load_user_config_effective(fail_closed=True).get('discord', {}).get('voice_engine', 'realtime')
        if engine not in {'realtime', 'transcription'}:
            raise ValueError('discord.voice_engine must be realtime or transcription')
        return engine == 'realtime'

    def capture_realtime_voice_attachment_factory(self):
        if not self.active:
            raise RuntimeError("Command invocation has expired")
        return VoiceIngressFactory(self.runner, self.source)

    async def capture_realtime_execution_attachment(self):
        if not self.active:
            raise RuntimeError("Command invocation has expired")
        from gateway.realtime_execution import RealtimeExecutionAttachment
        return await RealtimeExecutionAttachment(VoiceIngressFactory(self.runner, self.source)).prepare()

    def authorize_realtime_voice_control(self, *, guild_id):
        source = self.source
        if (not self.active or source.platform != Platform.DISCORD
                or int(source.scope_id or source.guild_id or 0) != guild_id
                or not self.runner._is_user_authorized_for_source(source)):
            raise PermissionError("Voice control belongs to its authorized Discord invocation")
        adapter = self.runner._intake_adapter_for(source)
        attachment = getattr(adapter, "_continuous_voice_sessions", {}).get(guild_id)
        if attachment is None or attachment.factory.text_channel_id != int(source.chat_id):
            raise PermissionError("Voice control belongs to the linked text channel")
        user_id = int(source.user_id or 0)
        if user_id != attachment.operator_user_id and not attachment.factory.admits(user_id):
            raise PermissionError("This speaker cannot control the canonical voice session")


class VoiceIngressFactory:
    """A single-use factory for a fixed live Discord connection and text binding."""

    transcription_ingress_version = 1

    def __init__(self, runner, source):
        from hermes_cli.config_effective import load_user_config_effective
        if source.platform != Platform.DISCORD or not runner._is_user_authorized_for_source(source):
            raise PermissionError("Canonical voice requires an authorized Discord invocation")
        self.runner, self.source = runner, replace_source(source)
        self.adapter = runner._intake_adapter_for(source)
        self.guild_id = int(source.scope_id or source.guild_id or 0)
        self.operator_user_id = int(source.user_id or 0)
        self.text_channel_id = int(source.chat_id)
        self.voice_client = getattr(self.adapter, "_voice_clients", {}).get(self.guild_id)
        self.channel_id = getattr(getattr(self.voice_client, "channel", None), "id", None)
        self._used = False
        with runner._profile_scope_for_source(self.source):
            config = load_user_config_effective(fail_closed=True)
            policy = config.get("discord", {}).get("voice_operator_only", True)
        if type(policy) is not bool:
            raise ValueError("discord.voice_operator_only must be a boolean")
        self.operator_only = policy
        self.check_binding()
        if not self.admits(self.operator_user_id):
            raise PermissionError("Join the bot's voice channel before starting canonical voice")

    def check_binding(self):
        if (self.voice_client is None or not self.voice_client.is_connected()
                or self.adapter._voice_clients.get(self.guild_id) is not self.voice_client
                or self.voice_client.channel.id != self.channel_id
                or self.adapter._voice_text_channels.get(self.guild_id) != self.text_channel_id
                or not callable(getattr(self.adapter, "_voice_input_callback", None))
                or self.runner._intake_adapter_for(self.source) is not self.adapter):
            raise RuntimeError("Discord voice connection or linked text channel changed")

    def scope(self):
        return self.runner._profile_scope_for_source(self.source)

    def realtime_source(self, user_id):
        """Re-resolve this speaker without borrowing the joiner's runtime identity."""
        from gateway.session_identity import canonical_identity, clear_identity
        source = replace_source(self.source, user_id=str(user_id), user_name=None,
                                role_authorized=False)
        clear_identity(source)
        identity = canonical_identity(source, runner=self.runner, adapter=self.adapter)
        bound = canonical_identity(self.source, runner=self.runner, adapter=self.adapter)
        if identity is None or bound is None or identity.runtime_profile != bound.runtime_profile:
            raise PermissionError('Voice speaker routes outside this attachment profile; rejoin on that profile')
        channel = self.voice_client.channel
        if getattr(self.adapter, '_allowed_role_ids', None):
            source.role_authorized = self.runner._under_authorization_profile(
                source, lambda: self.adapter._is_allowed_user(
                    str(user_id), guild=channel.guild, is_dm=False) is True)
        return source

    def admits(self, user_id):
        self.check_binding()
        if type(user_id) is not int or user_id <= 0:
            return False
        if self.operator_only and user_id != self.operator_user_id:
            return False
        channel = self.voice_client.channel
        member = next((m for m in channel.members if m.id == user_id and not m.bot), None)
        if member is None:
            return False
        with self.runner._profile_scope_for_source(self.source):
            return self.runner._under_authorization_profile(
                self.source,
                lambda: self.adapter._is_allowed_user(
                    str(user_id), guild=channel.guild, is_dm=False) is True,
            )

    async def open(self, provider_name):
        from agent.transcription_registry import get_provider
        from gateway.voice_ingress import VoiceIngressAttachment
        from hermes_cli.plugin_isolation import ISOLATION_HOST, isolation_mode
        if self._used:
            raise RuntimeError("Voice attachment factory has already been used")
        self.check_binding()
        if not self.admits(self.operator_user_id):
            raise PermissionError("Voice operator is no longer authorized in the channel")
        slots = getattr(self.adapter, "_continuous_voice_sessions", None)
        if slots is None:
            slots = self.adapter._continuous_voice_sessions = {}
        if self.guild_id in slots:
            raise RuntimeError("This voice channel already has a canonical attachment")
        with self.runner._profile_scope_for_source(self.source):
            if isolation_mode() == ISOLATION_HOST:
                raise RuntimeError("Continuous voice needs an in-process transcription provider")
            provider = get_provider(provider_name)
            if provider is None or provider.continuous_capable is not True:
                raise RuntimeError("No continuous transcription provider is registered in this profile")
            self._used = True
            attachment = VoiceIngressAttachment(self, provider)
            slots[self.guild_id] = attachment
            await attachment.start()
        return attachment

    async def deliver(self, user_id, text):
        # Recheck immediately before ingress, including membership/permissions
        # changed during transcription. The ordinary callback re-resolves each
        # speaker's route and goes through adapter.handle_message and both guards.
        if not self.admits(user_id):
            return
        with self.runner._profile_scope_for_source(self.source):
            await self.adapter._voice_input_callback(
                guild_id=self.guild_id, user_id=user_id, transcript=text)

    def release(self, attachment):
        slots = getattr(self.adapter, "_continuous_voice_sessions", {})
        if slots.get(self.guild_id) is attachment:
            del slots[self.guild_id]
