"""Real native admission, tool execution and history without a second LLM."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.mark.parametrize("profile,trusted", [("default", False), ("devops", False), ("devops", True)])
def test_realtime_tools_use_the_canonical_gateway_turn(tmp_path, profile, trusted):
    root = Path(__file__).resolve().parents[2]
    home = tmp_path / 'profile'
    home.mkdir()
    env = {k: os.environ[k] for k in ('PATH', 'LANG', 'TZ', 'SYSTEMROOT') if k in os.environ}
    env.update(HERMES_HOME=str(home), PYTHONPATH=str(root), PYTHONUNBUFFERED='1')
    result = subprocess.run([sys.executable, str(Path(__file__).parent / 'fixtures/realtime_execution_peer.py'),
                             profile, 'trusted' if trusted else 'strict'],
                            cwd=root, env=env, capture_output=True, text=True, timeout=80, check=False)
    assert result.returncode == 0, result.stdout + '\n' + result.stderr
    receipt = json.loads((home / 'duplex-receipt.json').read_text())
    assert receipt['completed'] and receipt['durable_turns'] == 1
    assert receipt['history_roles'][-3:] == ['assistant', 'tool', 'assistant']


def test_expired_voice_request_never_falls_back_to_a_regular_model():
    from types import SimpleNamespace
    from gateway.realtime_execution import run_voice_conversation
    called = []
    agent = SimpleNamespace(run_conversation=lambda *args, **kwargs: called.append(args))
    with pytest.raises(RuntimeError, match='expired'):
        run_voice_conversation(agent, 'execute this', {}, message_id='realtime-voice:expired', source=None)
    assert called == []
