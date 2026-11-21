"""The voice transport must never be inherited by a delegated agent."""
from agent.external_completion import bind_completion, transport_for


def test_child_and_later_turn_cannot_use_parent_voice_transport():
    parent, child, complete = object(), object(), object()
    with bind_completion(parent, complete):
        assert transport_for(parent) is complete
        assert transport_for(child) is None
        with bind_completion(child, 'child'):
            assert transport_for(child) == 'child'
            assert transport_for(parent) is None
        assert transport_for(parent) is complete
    assert transport_for(parent) is None
