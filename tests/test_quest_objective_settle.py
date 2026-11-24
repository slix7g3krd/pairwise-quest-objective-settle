import pytest
import gameplay.quest_objective_settle as mod

CASES = [
    ("event_dedupe", (1,)),
    ("listener_lifetime", (1,)),
    ("offline_sequence", (1,)),
    ("single_complete_event", (1,)),
    ("rollback_snapshot", (1,)),
    ("determinism", (1,)),
    ("idempotent_progress", (1,)),
    ("validation", (1,)),
    ("audit", (1,)),
    ("fallback", (1,)),
    ("threshold_check", (1,)),
    ("metrics", (1,)),
    ("quest_apply", (1,)),
    ("reward_grant", (1,)),
    ("progress_merge", (1,)),
]

@pytest.mark.parametrize("fn_name,args", CASES)
def test_contract(fn_name, args):
    fn = getattr(mod, fn_name)
    assert fn(*args) is not None
