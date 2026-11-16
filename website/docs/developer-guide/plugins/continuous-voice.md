# Discord voice: Realtime execution and continuous transcription


## Realtime model with canonical execution

`PluginVoiceInvocation.realtime_execution_version == 1` advertises the live
`await invocation.capture_realtime_execution_attachment()` capability. Capture
must occur during the authorized Discord command. The attachment binds the exact
adapter, connection, room, text conversation, operator and profile; the same speaker
policy below applies. `discord.voice_engine` defaults to `realtime`; `transcription`
selects the separate ingress described below.

The provider retains duplex audio and barge-in. The attachment exposes canonical
`tool_definitions()` and bounded `initial_history()` (user/assistant text only,
20 messages/16k characters). History must be replayed as conversation data, never
system instructions. Final input is reported using `note_input(user_id, text,
input_item_id=...)`. A trusted PCM/VAD ledger must bind every response and function
call to that exact Discord speaker and input item, including continuations.

`mint_tool_call_permit(response_id=..., item_id=..., call_id=..., batch_id=...,
tool_name=..., arguments=..., speaker_user_id=..., input_item_id=...)` returns a
single-use opaque permit. `await execute_tool_batch(permits)` returns exact tool
outputs. `note_response(response_id, user_id, text, has_tools=False,
input_item_id=...)` closes the corresponding native turn. `close()` revokes the
attachment. Audio processing must run independently of those awaited tool tasks.

Execution re-enters the receiving adapter’s registered message callback, which
establishes native provenance for the new event, then ordinary gateway admission and TurnRunner, then supplies the
external model completion to the normal agent loop. Tool checks, approvals, skill
hooks, conversation persistence and child-agent execution remain canonical. The
completion binding belongs only to the parent agent; children retain their own
model. No raw dispatcher and no second inference to reinterpret the tool request
are used. Native streaming/text delivery and TTS acknowledgements are suppressed
for these audio-owned turns. The first implementation requires the host agent's
`chat_completions` protocol and refuses other protocols.

A native turn waits up to 30 seconds for its exact final input transcript before
execution. New speech closes the earlier response with an interruption marker;
it does not undo running tools. Provider continuations wait at most 180 seconds.
Live capabilities are not replayable: a durable admission left after restart fails
closed if its in-memory voice binding expired. A rejoin is required. The initial
history snapshot is bounded and does not continuously synchronize other writers.

### Optional trusted owner room

`discord.voice_trusted_room: true` exposes `attachment.trusted_room` to the consumer.
The consumer attributes room audio to `attachment.operator_user_id`, including
mixed/unresolved speaker audio, and omits spoken-permit expiry. The live receiving
connection, operator and runtime profile remain bound. Default false preserves
per-speaker attribution. `attachment.approvals_disabled` reports whether the
profile's existing `approvals.mode` is off; that setting controls canonical approval
gates, rather than introducing a second executor or an approval policy in Talk.
The consumer should explain unconfirmed tool execution without ending audio or
claiming that an uncertain operation had no effects.

## Continuous transcription ingress

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
