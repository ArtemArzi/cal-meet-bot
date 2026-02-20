import re
with open("tests/test_phase7_side_effects.py", "r") as f:
    c = f.read()

target_test = "def test_repeated_same_decision_does_not_enqueue_duplicate_calendar_patch"

idx = c.find(target_test)
if idx != -1:
    end_idx = c.find("def test_", idx + len(target_test))
    if end_idx == -1: end_idx = len(c)
    
    test_body = c[idx:end_idx]
    test_body = test_body.replace("assert second.result.outcome == Outcome.OK", "assert second.result.outcome == Outcome.NOOP")
    
    c = c[:idx] + test_body + c[end_idx:]
    with open("tests/test_phase7_side_effects.py", "w") as f:
        f.write(c)
