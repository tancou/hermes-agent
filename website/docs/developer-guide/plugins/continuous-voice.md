# Continuous voice ingress for Discord

This extension lets a plugin transport live speech into the existing Discord
conversation. Hermes Talk is the first consumer. It uses the existing
`TranscriptionProvider` registration and the ordinary gateway voice callback;
the provider never gets agent tools, approvals, history, or another user's identity.

The public optional `TranscriptionProvider.continuous_capable` property defaults
to false. An implementing provider returns a `ContinuousTranscriptionSession`
from `await open_continuous_session()`. The session accepts 24 kHz mono PCM16LE,
emits `TranscriptionEvent` values, and closes idempotently. `ready` means its
configuration has been acknowledged, `speech_started` is informational, and only
a final `transcript` enters the gateway. Item IDs deduplicate final deliveries;
a conflicting final fails the attachment. Errors and unexpected EOF end it.
The existing synchronous, single-utterance streaming API is unchanged.

A Discord plugin command opts into the live context by declaring the keyword
`invocation`. During command execution it can call
`invocation.capture_realtime_voice_attachment_factory()`. The returned factory
has `transcription_ingress_version == 1` and `await factory.open(provider_name)`.
Capture expires when the command returns; an already captured factory is
single-use. This also works when a synchronous handler returns a coroutine from
the gateway worker to the owning event loop. Other surfaces have no voice factory.

The factory binds the receiving adapter, exact voice connection, voice channel,
linked text channel, operator and profile. It refuses capture unless the caller
is authorized and currently in that voice channel. Rebinding/reconnecting revokes
the attachment; start another session. Its async `feed_audio(pcm,
speaker_user_id=...)` accepts transport-attributed audio; synthesized silence must
contain only zero samples. Unresolved speakers are discarded. Each admitted
speaker has a separate STT socket, avoiding cross-speaker transcript attribution.
The scoped provider registry and every asynchronous provider operation use the
captured profile, including lazily opened streams for additional speakers.

Final transcripts re-enter `_handle_voice_channel_input` and
`adapter.handle_message`: the existing per-speaker routing, both busy guards,
session history, memory, MCP/tool selection, approvals and TTS remain in charge.
No subprocess agent or alternative tool executor is introduced. Configuration
changes to prompts/tools follow the existing session rules. Native speech replies
use the host's configured TTS, not the realtime transcription model's voice.

## Speaker policy

`discord.voice_operator_only` defaults to `true`. With `false`, other **currently
authorized human members of this voice channel** may talk to Hermes. This is the
opt-out for private, restricted Discord rooms. It does not bypass Discord/Gateway
allowlists, role checks, profile routing or normal tool approvals. Malformed
values refuse startup instead of being interpreted as false. It is read from the
owning profile's effective configuration at session start, including managed
overlays; restart the voice session after changing it.

```yaml
discord:
  voice_operator_only: false
```

Controls use `invocation.authorize_realtime_voice_control(guild_id=...)`. They
must originate in the linked text channel: the initiating user may always stop
their attachment, and in the relaxed mode another authorized room member may
control it. A different guild or text channel cannot control that attachment.

## Limits and lifecycle

This is continuous STT feeding native agent turns, not the unmerged realtime
provider API-v2 orchestrator. It does not advertise that contract or implement
provider-owned tool batches. Speech-start events do not interrupt running tools;
completed utterances follow the gateway's ordinary busy/steering behavior. Use
the existing typed stop controls when an immediate agent stop is needed.

At most eight speakers, 4096 final item IDs, and 32 queued transcripts are held
per attachment. PCM calls are limited to one second; provider operations/readiness
have 30-second deadlines. Exceeding a bound fails visibly rather than evicting
deduplication records and replaying a command. Closing cancels ingress tasks,
closes all provider sockets and releases the room lease. It does not claim to
undo tool side effects already accepted by a canonical turn.

Continuous sessions currently require `plugins.isolation: in_process`. A remote
plugin-host proxy cannot carry a live asynchronous session and is refused. No
reconnection is automatic. Provider/model credentials and Discord audio still
need a live microphone acceptance test; offline tests cover simulated wire events
and the real gateway command/voice ingress, not Discord delivery or live billing.
