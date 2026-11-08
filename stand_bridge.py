"""Stand-side bridge between a LabVIEW VI and a configured RabbitMQ broker.

Called from a LabVIEW Python Node once per control-loop iteration:

    exchange(headers, values, device_id, location)
        -> (verb, target, value, command_id, status)

Each call does two things over a single cached broker connection:

1. Publishes every parseable channel in ``headers``/``values`` as telemetry
   (one JSON message per channel to exchange ``amq.topic``, routing key
   ``labview.<measurement>``).
2. Polls the durable per-device command queue
   (``labview_commands.<device_id>``) and returns at most one validated
   command. ``verb == ""`` means "no command this tick".

Commands require an explicit external policy (verb/target, value bounds,
TTL and per-target rate limit). With no policy, no commands are returned.
Invalid commands are dropped, logged, and rejected via an ack message on
``labview.cmd_ack.<device_id>``; valid ones are acked as "delivered".

This module NEVER raises to LabVIEW — all failures are reported in the
``status`` element of the returned tuple and logged to ``lv_agent.log``.

Delivery semantics toward hardware are at-most-once: messages are acked on
receipt, so a command lost after ack is never re-applied. Agents that care
should watch telemetry (the authoritative signal) or the cmd_ack stream and
re-issue.
"""

import datetime
import json
import logging
import math
import os
import re
import time
import tomllib
from pathlib import Path

import pika

_HERE = Path(__file__).parent


def _load_env_file() -> None:
    """Load KEY=VALUE lines from a local .env (gitignored) into os.environ.

    LabVIEW launched from the macOS Dock / Windows Explorer does not inherit
    shell exports, so broker credentials live in a file next to this module.
    Existing environment variables win.
    """
    env_file = _HERE / ".env"
    if env_file.is_file():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip())


_load_env_file()

_RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
_RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", "5672"))
_RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest")
_RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "guest")
_EXCHANGE = "amq.topic"

_LOG_PATH = os.environ.get("LV_AGENT_LOG", str(_HERE / "lv_agent.log"))

# Matches the trailing "( unit )" telemetry-header convention,
# e.g. "Lamp 1 Voltage( V)" -> ("Lamp 1 Voltage", "V").
_HEADER_RE = re.compile(r"^(.*?)\s*\(\s*(.+?)\s*\)\s*$")

_NO_COMMAND = ("", "", 0.0, "")

# Opt-in only: this file is not bundled with an active policy. A missing,
# unreadable, or malformed policy rejects every command (including queued ones).
_POLICY_PATH = os.environ.get("LV_COMMAND_POLICY", "")


def _load_policy(path: str) -> dict[tuple[str, str], tuple[float, float, float, float]]:
    """Read and validate the entire TOML policy; any error fails closed."""
    if not path:
        return {}
    document = tomllib.loads(Path(path).read_text(encoding="utf-8"))
    if set(document) != {"commands"} or not isinstance(document["commands"], list):
        raise ValueError("policy must contain only [[commands]] entries")
    result = {}
    for entry in document["commands"]:
        if not isinstance(entry, dict) or set(entry) != {
            "verb", "target", "min_value", "max_value", "max_ttl_seconds", "min_interval_seconds"
        }:
            raise ValueError("invalid command policy entry")
        verb, target = entry["verb"], entry["target"]
        if not isinstance(verb, str) or not verb or not isinstance(target, str) or not target:
            raise ValueError("policy verb and target must be nonempty strings")
        numbers = [entry[k] for k in (
            "min_value", "max_value", "max_ttl_seconds", "min_interval_seconds"
        )]
        if any(isinstance(n, bool) or not isinstance(n, (int, float)) or
               not math.isfinite(n) for n in numbers):
            raise ValueError("policy limits must be finite numbers")
        low, high, max_ttl, interval = map(float, numbers)
        if low > high or not 0 < max_ttl <= 300 or interval < 0 or (verb, target) in result:
            raise ValueError("invalid or duplicate command policy limits")
        result[(verb, target)] = (low, high, max_ttl, interval)
    return result

# Most commands drained (and validated or dropped) per exchange() call, so a
# queue full of garbage can't stall the loop.
_MAX_DRAIN_PER_TICK = 10

logging.getLogger("pika").setLevel(logging.WARNING)

_last_applied: dict[str, float] = {}
_connection: pika.BlockingConnection | None = None
_channel: pika.adapters.blocking_connection.BlockingChannel | None = None
_declared_queues: set[str] = set()


def _get_logger() -> logging.Logger:
    logger = logging.getLogger("lv_agent")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        try:
            handler: logging.Handler = logging.FileHandler(_LOG_PATH)
        except OSError:
            handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(handler)
    return logger


def _sanitize(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]", "_", name).strip("_")


def _routing_key(measurement: str) -> str:
    return "labview." + _sanitize(measurement)


def _reset_connection() -> None:
    global _connection, _channel
    try:
        if _connection is not None and _connection.is_open:
            _connection.close()
    except Exception:
        pass
    _connection = None
    _channel = None
    _declared_queues.clear()


def _get_channel() -> "pika.adapters.blocking_connection.BlockingChannel":
    """Return a live channel, reusing the cached connection when possible."""
    global _connection, _channel
    if (
        _channel is not None
        and _channel.is_open
        and _connection is not None
        and _connection.is_open
    ):
        return _channel
    _reset_connection()
    params = pika.ConnectionParameters(
        host=_RABBITMQ_HOST,
        port=_RABBITMQ_PORT,
        credentials=pika.PlainCredentials(_RABBITMQ_USER, _RABBITMQ_PASS),
        heartbeat=30,
        blocked_connection_timeout=10,
        socket_timeout=5,
    )
    _connection = pika.BlockingConnection(params)
    _channel = _connection.channel()
    return _channel


def _ensure_command_queue(channel, device_id: str) -> str:
    queue = f"labview_commands.{_sanitize(device_id)}"
    if queue not in _declared_queues:
        channel.queue_declare(queue=queue, durable=True)
        channel.queue_bind(
            queue=queue,
            exchange=_EXCHANGE,
            routing_key=f"labview.cmd.{_sanitize(device_id)}",
        )
        _declared_queues.add(queue)
    return queue


def validate_command(
    cmd: dict,
    device_id: str,
    *,
    now: datetime.datetime | None = None,
    last_applied: dict[str, float] | None = None,
    monotonic_now: float | None = None,
) -> tuple[tuple[str, str, float, str] | None, str]:
    """Validate a decoded command dict against the contract.

    Returns ``((verb, target, value, command_id), "")`` on success or
    ``(None, reason)`` on rejection. Pure apart from the injected clocks/state
    so it is unit-testable.
    """
    now = now or datetime.datetime.now(datetime.UTC)
    last_applied = _last_applied if last_applied is None else last_applied
    monotonic_now = time.monotonic() if monotonic_now is None else monotonic_now

    try:
        policy = _load_policy(_POLICY_PATH)
    except (OSError, UnicodeError, ValueError) as exc:
        return None, f"policy unavailable/invalid: {type(exc).__name__}"
    if not policy:
        return None, "no command policy configured"
    if not isinstance(cmd, dict):
        return None, "command must be an object"

    command_id = str(cmd.get("command_id", ""))
    verb = str(cmd.get("verb", ""))
    target = str(cmd.get("target", ""))

    if cmd.get("device_id") != device_id:
        return None, f"device_id mismatch (got {cmd.get('device_id')!r})"

    issued_at_raw = cmd.get("issued_at")
    if not issued_at_raw:
        return None, "missing issued_at"
    try:
        issued_at = datetime.datetime.fromisoformat(str(issued_at_raw))
    except ValueError:
        return None, f"unparseable issued_at {issued_at_raw!r}"
    if issued_at.tzinfo is None:
        return None, "issued_at must include a timezone"
    try:
        issued_at = issued_at.astimezone(datetime.UTC)
    except OverflowError:
        return None, "issued_at out of range"
    bounds = policy.get((verb, target))
    if bounds is None:
        return None, f"not allowed: {verb!r} {target!r}"
    ttl_raw = cmd.get("ttl_seconds")
    if isinstance(ttl_raw, bool) or not isinstance(ttl_raw, (int, float)):
        return None, "missing/invalid ttl_seconds"
    try:
        ttl = float(ttl_raw)
    except OverflowError:
        return None, "missing/invalid ttl_seconds"
    if not math.isfinite(ttl) or ttl <= 0 or ttl > bounds[2]:
        return None, "ttl_seconds outside policy limit"
    if issued_at > now:
        return None, "issued_at is in the future"
    try:
        expires_at = issued_at + datetime.timedelta(seconds=ttl)
    except OverflowError:
        return None, "issued_at out of range"
    if now > expires_at:
        return None, f"stale (issued {issued_at.isoformat()}, ttl {ttl}s)"

    value_raw = cmd.get("value")
    if isinstance(value_raw, bool) or not isinstance(value_raw, (int, float)):
        return None, "non-numeric value"
    try:
        value = float(value_raw)
    except OverflowError:
        return None, "non-numeric value"
    if not math.isfinite(value) or not bounds[0] <= value <= bounds[1]:
        return None, "value outside policy bounds"

    min_interval = bounds[3]
    last = last_applied.get(target)
    if min_interval > 0 and last is not None and monotonic_now - last < min_interval:
        return None, f"rate-limited ({target}: min {min_interval}s between commands)"

    last_applied[target] = monotonic_now
    return (verb, target, value, command_id), ""


def _publish_json(channel, routing_key: str, payload: dict) -> None:
    channel.basic_publish(
        exchange=_EXCHANGE,
        routing_key=routing_key,
        body=json.dumps(payload),
        properties=pika.BasicProperties(content_type="application/json"),
    )


def _publish_telemetry(
    channel, headers: list[str], values: list[float], device_id: str, location: str
) -> int:
    timestamp = datetime.datetime.now(datetime.UTC).isoformat()
    published = 0
    for header, val in zip(headers, values, strict=True):
        parsed = _HEADER_RE.match(header.strip())
        if parsed is None:
            continue  # headers without a "( unit )" suffix (e.g. Timestamp)
        measurement, unit = parsed.group(1).strip(), parsed.group(2).strip()
        _publish_json(
            channel,
            _routing_key(measurement),
            {
                "timestamp": timestamp,
                "measurement": measurement,
                "value": float(val),
                "unit": unit,
                "device_id": device_id,
                "location": location,
                "source": "labview",
            },
        )
        published += 1
    return published


def _publish_ack(channel, device_id: str, command_id: str, status: str, reason: str) -> bool:
    # Rejection receipts are best-effort. A failure to enqueue a delivery
    # receipt prevents returning an accepted command to the VI.
    try:
        _publish_json(
            channel,
            f"labview.cmd_ack.{_sanitize(device_id)}",
            {
                "command_id": command_id,
                "device_id": device_id,
                "status": status,
                "reason": reason,
                "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
            },
        )
    except Exception:
        return False
    return True


def _poll_command(channel, device_id: str, log: logging.Logger) -> tuple[str, str, float, str]:
    """Drain the command queue; return the first valid command (or no-op)."""
    queue = _ensure_command_queue(channel, device_id)
    for _ in range(_MAX_DRAIN_PER_TICK):
        method, _props, body = channel.basic_get(queue=queue, auto_ack=False)
        if method is None:
            return _NO_COMMAND
        channel.basic_ack(method.delivery_tag)  # at-most-once toward hardware
        try:
            cmd = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            log.warning("dropped non-JSON command: %s", e)
            continue
        result, reason = validate_command(cmd, device_id)
        command_id = str(cmd.get("command_id", "")) if isinstance(cmd, dict) else ""
        if result is None:
            log.warning("rejected command %s: %s", command_id or "<no id>", reason)
            _publish_ack(channel, device_id, command_id, "rejected", reason)
            continue
        if not _publish_ack(channel, device_id, command_id, "delivered", ""):
            log.error("command %s dropped: broker receipt publish failed", command_id)
            return _NO_COMMAND
        log.info("delivering command %s: %s %s = %s", command_id, *result[:3])
        return result
    return _NO_COMMAND


def exchange(
    headers: list[str] | str,
    values: list[float],
    device_id: str,
    location: str,
) -> tuple[str, str, float, str, str]:
    """Single LabVIEW Python Node entry point — one call per loop iteration.

    ``headers`` may be a list of strings or ONE ``;``-separated string
    ("Lamp 1 Voltage( V);Lamp 2 Voltage( V);..."), which is much easier to
    wire in LabVIEW (a single string constant instead of an array constant).

    Returns ``(verb, target, value, command_id, status)``; ``verb == ""``
    means no command this tick. Never raises.
    """
    log = _get_logger()

    if isinstance(headers, str):
        headers = [h.strip() for h in headers.split(";") if h.strip()]
    elif not isinstance(headers, list):
        return (*_NO_COMMAND, "ERROR (validation): headers must be a list or string")
    if not isinstance(values, (list, tuple)):
        return (*_NO_COMMAND, "ERROR (validation): values must be a list")
    if len(headers) != len(values):
        return (
            *_NO_COMMAND,
            f"ERROR (validation): headers/values length mismatch ({len(headers)} vs {len(values)})",
        )

    try:
        channel = _get_channel()
    except Exception as e:
        _reset_connection()
        log.error("connect failed: %s", e)
        return (*_NO_COMMAND, f"ERROR (connect): {e}")

    try:
        published = _publish_telemetry(channel, headers, values, device_id, location)
    except Exception as e:
        _reset_connection()
        log.error("publish failed: %s", e)
        return (*_NO_COMMAND, f"ERROR (publish): {e}")

    try:
        command = _poll_command(channel, device_id, log)
    except Exception as e:
        _reset_connection()
        log.error("command poll failed: %s", e)
        return (*_NO_COMMAND, f"ERROR: {published} published | ERROR (poll): {e}")

    cmd_desc = f"cmd={command[0]} {command[1]}={command[2]}" if command[0] else "no cmd"
    return (*command, f"OK: {published} published | {cmd_desc}")


def publish_only(
    headers: list[str] | str,
    values: list[float],
    device_id: str,
    location: str,
) -> None:
    """Telemetry-only variant for first wire-up: publishes but NEVER polls
    the command queue, and returns None — usable while the Python Node's
    return type is still the default (None). Status goes to lv_agent.log.

    Switch the VI's function-name constant to ``exchange`` once the return
    cluster is wired; until then no command is ever consumed, so nothing
    an agent sends can be silently eaten during bring-up.
    """
    log = _get_logger()

    if isinstance(headers, str):
        headers = [h.strip() for h in headers.split(";") if h.strip()]
    if not isinstance(headers, list) or not isinstance(values, (list, tuple)):
        log.error("publish_only: headers must be a list or string and values a list")
        return
    if len(headers) != len(values):
        log.error(
            "publish_only: headers/values length mismatch (%d vs %d)", len(headers), len(values)
        )
        return
    try:
        channel = _get_channel()
        published = _publish_telemetry(channel, headers, values, device_id, location)
        log.info("publish_only: %d published", published)
    except Exception as e:
        _reset_connection()
        log.error("publish_only failed: %s", e)
