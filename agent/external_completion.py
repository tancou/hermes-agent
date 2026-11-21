"""A host-bound completion transport, limited to one particular agent turn.

The normal conversation loop still validates and executes every tool call. A
child agent inherits contextvars but never inherits its parent's transport.
"""
from contextlib import contextmanager
from contextvars import ContextVar

_transport = ContextVar('external_completion', default=None)


@contextmanager
def bind_completion(agent, complete):
    token = _transport.set((agent, complete))
    try:
        yield
    finally:
        _transport.reset(token)


def transport_for(agent):
    current = _transport.get()
    return current[1] if current is not None and current[0] is agent else None
