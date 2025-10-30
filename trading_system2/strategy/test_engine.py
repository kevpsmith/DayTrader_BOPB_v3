from datetime import datetime, timezone
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.strategy.engine import StrategyState, TradeInstruction, evaluate


@pytest.fixture
def baseline_timestamp():
    return datetime(2024, 3, 14, 9, 30, tzinfo=timezone.utc)


def make_bar(ts, price=100.0, entry=False, exit=False):
    return {
        "timestamp": ts,
        "price": price,
        "entry_signal": entry,
        "exit_signal": exit,
    }


def test_entry_signal_executes_when_flat(baseline_timestamp):
    state = StrategyState()
    bar = make_bar(baseline_timestamp, entry=True, exit=True)

    instruction = evaluate("AAPL", bar, state)

    assert isinstance(instruction, TradeInstruction)
    assert instruction.action == "buy"
    assert state.position == "long"
    assert state.last_action_timestamp == baseline_timestamp


def test_exit_ignored_immediately_after_entry(baseline_timestamp):
    state = StrategyState()
    entry_bar = make_bar(baseline_timestamp, entry=True)
    exit_bar = make_bar(baseline_timestamp, exit=True)

    entry_instruction = evaluate("AAPL", entry_bar, state)
    assert entry_instruction and entry_instruction.action == "buy"

    exit_instruction = evaluate("AAPL", exit_bar, state)
    assert exit_instruction is None
    assert state.position == "long"
    assert state.last_action_timestamp == baseline_timestamp


def test_exit_allowed_on_future_bar(baseline_timestamp):
    state = StrategyState()
    entry_bar = make_bar(baseline_timestamp, entry=True)
    later_bar = make_bar(baseline_timestamp.replace(minute=31), exit=True)

    evaluate("AAPL", entry_bar, state)
    instruction = evaluate("AAPL", later_bar, state)

    assert isinstance(instruction, TradeInstruction)
    assert instruction.action == "sell"
    assert state.position is None
    assert state.last_action_timestamp == later_bar["timestamp"]


def test_back_to_back_entries_do_not_duplicate_state(baseline_timestamp):
    state = StrategyState()
    first_bar = make_bar(baseline_timestamp, entry=True)
    second_bar = make_bar(baseline_timestamp.replace(minute=31), entry=True)

    first_instruction = evaluate("AAPL", first_bar, state)
    assert first_instruction and first_instruction.action == "buy"

    second_instruction = evaluate("AAPL", second_bar, state)
    assert second_instruction is None
    assert state.position == "long"
    assert state.last_action_timestamp == baseline_timestamp


def test_exit_prioritised_when_both_signals_present_while_long(baseline_timestamp):
    state = StrategyState()
    entry_bar = make_bar(baseline_timestamp, entry=True)
    evaluate("AAPL", entry_bar, state)

    exit_bar = make_bar(baseline_timestamp.replace(minute=35), entry=True, exit=True)
    instruction = evaluate("AAPL", exit_bar, state)

    assert instruction and instruction.action == "sell"
    assert state.position is None