"""Offline policy and bridge safety tests."""

import datetime
import json
import tomllib
from pathlib import Path

import pytest

import stand_bridge as bridge

NOW = datetime.datetime(2026, 7, 17, 16, 0, tzinfo=datetime.UTC)
POLICY = '''[[commands]]
verb = "set"
target = "lamp_1"
min_value = 0.0
max_value = 1.0
max_ttl_seconds = 10.0
min_interval_seconds = 10.0
'''


def make_cmd(**overrides):
    cmd = {"command_id": "test-1", "issued_at": NOW.isoformat(), "ttl_seconds": 10,
           "device_id": "demo", "verb": "set", "target": "lamp_1", "value": 1.0}
    cmd.update(overrides)
    return cmd


def validate(cmd, **kwargs):
    return bridge.validate_command(cmd, "demo", now=NOW, last_applied={}, monotonic_now=100,
                                   **kwargs)


@pytest.fixture
def enabled_policy(tmp_path, monkeypatch):
    policy = tmp_path / "policy.toml"
    policy.write_text(POLICY)
    monkeypatch.setattr(bridge, "_POLICY_PATH", str(policy))
    return policy


def test_default_policy_never_returns_commands(monkeypatch):
    monkeypatch.setattr(bridge, "_POLICY_PATH", "")
    result, reason = validate(make_cmd())
    assert result is None and "no command policy" in reason


def test_allowed_passes(enabled_policy):
    assert validate(make_cmd()) == (("set", "lamp_1", 1.0, "test-1"), "")


@pytest.mark.parametrize("overrides,reason", [
    ({"target": "unknown"}, "not allowed"),
    ({"value": 2}, "bounds"),
    ({"value": float("nan")}, "bounds"),
    ({"ttl_seconds": 11}, "policy limit"),
    ({"ttl_seconds": float("inf")}, "policy limit"),
    ({"issued_at": (NOW - datetime.timedelta(seconds=11)).isoformat()}, "stale"),
    ({"issued_at": (NOW + datetime.timedelta(seconds=1)).isoformat()}, "future"),
    ({"issued_at": "2026-07-17T16:00:00"}, "timezone"),
    ({"device_id": "other"}, "device_id mismatch"),
])
def test_disallowed_and_expired_rejected(enabled_policy, overrides, reason):
    result, message = validate(make_cmd(**overrides))
    assert result is None and reason in message


@pytest.mark.parametrize("issued_at", [
    "0001-01-01T00:00:00+14:00",
    "9999-12-31T23:59:59-10:00",
])
def test_issued_at_utc_conversion_out_of_range(enabled_policy, issued_at):
    result, reason = validate(make_cmd(issued_at=issued_at))
    assert result is None and reason == "issued_at out of range"


def test_issued_at_expiration_out_of_range(enabled_policy):
    near_max = datetime.datetime(9999, 12, 31, 23, 59, 58, tzinfo=datetime.UTC)
    result, reason = bridge.validate_command(
        make_cmd(issued_at="9999-12-31T23:59:55+00:00"),
        "demo", now=near_max, last_applied={}, monotonic_now=100,
    )
    assert result is None and reason == "issued_at out of range"


@pytest.mark.parametrize("bad", [True, False, "5", None, [], {}])
def test_ttl_requires_numeric_type(enabled_policy, bad):
    result, reason = validate(make_cmd(ttl_seconds=bad))
    assert result is None and reason == "missing/invalid ttl_seconds"


@pytest.mark.parametrize("bad", [True, False, "1.0", None, [], {}])
def test_value_requires_numeric_type(enabled_policy, bad):
    result, reason = validate(make_cmd(value=bad))
    assert result is None and reason == "non-numeric value"


def test_example_policy_only_lists_three_synthetic_lamps():
    path = Path(__file__).resolve().parent.parent / "examples/policies/lamp-stand.example.toml"
    policy = tomllib.loads(path.read_text())
    assert {(item["verb"], item["target"]) for item in policy["commands"]} == {
        ("set", "lamp_1"), ("set", "lamp_2"), ("set", "lamp_3"),
    }


def test_rate_limit(enabled_policy):
    state = {}
    first, _ = bridge.validate_command(make_cmd(), "demo", now=NOW,
                                       last_applied=state, monotonic_now=100)
    second, reason = bridge.validate_command(make_cmd(), "demo", now=NOW,
                                              last_applied=state, monotonic_now=105)
    third, _ = bridge.validate_command(make_cmd(), "demo", now=NOW,
                                       last_applied=state, monotonic_now=110)
    assert first and second is None and "rate-limited" in reason and third


@pytest.mark.parametrize("replacement", [
    '[[commands]]\nverb = "set"\n',
    POLICY + POLICY,
    POLICY.replace("max_ttl_seconds = 10.0", 'max_ttl_seconds = "unlimited"'),
])
def test_malformed_policy_fails_closed(enabled_policy, replacement):
    enabled_policy.write_text(replacement)
    result, reason = validate(make_cmd())
    assert result is None and "policy" in reason


def test_missing_policy_fails_closed(enabled_policy):
    enabled_policy.unlink()
    result, reason = validate(make_cmd())
    assert result is None and "policy" in reason


def test_poll_without_policy_consumes_but_never_delivers(monkeypatch):
    monkeypatch.setattr(bridge, "_POLICY_PATH", "")
    monkeypatch.setattr(bridge, "_declared_queues", set())

    class Channel:
        def __init__(self):
            self.pending = [json.dumps(make_cmd()).encode()]
            self.acks = []

        def queue_declare(self, **_kwargs):
            pass

        def queue_bind(self, **_kwargs):
            pass

        def basic_get(self, **_kwargs):
            if self.pending:
                return type("Delivery", (), {"delivery_tag": 1})(), None, self.pending.pop()
            return None, None, None

        def basic_ack(self, tag):
            self.acks.append(tag)

        def basic_publish(self, **_kwargs):
            pass

    ch = Channel()
    result = bridge._poll_command(ch, "demo", bridge._get_logger())
    assert result == bridge._NO_COMMAND and ch.acks == [1]


def test_receipt_publish_failure_drops_allowed_command(enabled_policy, monkeypatch):
    monkeypatch.setattr(bridge, "_declared_queues", set())

    class Channel:
        def __init__(self):
            self.pending = [json.dumps(make_cmd()).encode()]

        def queue_declare(self, **_kwargs):
            pass

        def queue_bind(self, **_kwargs):
            pass

        def basic_get(self, **_kwargs):
            if self.pending:
                return type("Delivery", (), {"delivery_tag": 1})(), None, self.pending.pop()
            return None, None, None

        def basic_ack(self, _tag):
            pass

        def basic_publish(self, **_kwargs):
            raise OSError("broker publish failed")

    assert bridge._poll_command(Channel(), "demo", bridge._get_logger()) == bridge._NO_COMMAND


def test_broker_failure_fails_safe(monkeypatch):
    monkeypatch.setattr(bridge, "_get_channel", lambda: (_ for _ in ()).throw(OSError("offline")))
    result = bridge.exchange(["Temperature( C)"], [22.0], "demo", "simulated")
    assert result[:4] == bridge._NO_COMMAND and "ERROR (connect)" in result[4]


@pytest.mark.parametrize("headers", [None, 1, {}, ("Temperature( C)",)])
def test_invalid_header_type_fails_validation_without_connecting(monkeypatch, headers):
    def unexpected_connection():
        pytest.fail("invalid headers must not open a broker connection")

    monkeypatch.setattr(bridge, "_get_channel", unexpected_connection)
    result = bridge.exchange(headers, [22.0], "demo", "simulated")
    assert result[:4] == bridge._NO_COMMAND
    assert result[4].startswith("ERROR (validation): headers must be a list or string")


@pytest.mark.parametrize("values", [None, 5, "x"])
def test_invalid_values_type_fails_validation_without_connecting(monkeypatch, values):
    def unexpected_connection():
        pytest.fail("invalid values must not open a broker connection")

    monkeypatch.setattr(bridge, "_get_channel", unexpected_connection)
    result = bridge.exchange(["Temperature( C)"], values, "demo", "simulated")
    assert result[:4] == bridge._NO_COMMAND
    assert result[4].startswith("ERROR (validation): values must be a list")


@pytest.mark.parametrize("headers,values", [(None, []), (["a( V)"], None), (5, 5)])
def test_publish_only_never_raises_on_bad_types(monkeypatch, headers, values):
    def unexpected_connection():
        pytest.fail("invalid input must not open a broker connection")

    monkeypatch.setattr(bridge, "_get_channel", unexpected_connection)
    assert bridge.publish_only(headers, values, "demo", "simulated") is None


def test_poll_failure_fails_safe(monkeypatch):
    monkeypatch.setattr(bridge, "_get_channel", lambda: object())
    monkeypatch.setattr(bridge, "_publish_telemetry", lambda *_: 1)
    monkeypatch.setattr(bridge, "_poll_command", lambda *_: (
        _ for _ in ()
    ).throw(OSError("offline")))
    result = bridge.exchange("Temperature( C);", [22.0], "demo", "simulated")
    assert result[:4] == bridge._NO_COMMAND
    assert result[4].startswith("ERROR: 1 published | ERROR (poll): offline")


def test_header_convention():
    assert bridge._HEADER_RE.match("Temperature( C)").groups() == ("Temperature", "C")
    assert bridge._HEADER_RE.match("Timestamp") is None
