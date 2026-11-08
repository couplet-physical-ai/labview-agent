# labview-agent

A Python bridge for a LabVIEW VI to publish telemetry to RabbitMQ and optionally receive policy-filtered command proposals, one VI tick at a time.

![Status: provided as-is](https://img.shields.io/badge/status-provided--as--is-lightgrey) ![License: Apache--2.0](https://img.shields.io/badge/license-Apache--2.0-blue)

Created at [Exowatt](https://exowatt.com). The project is governed independently in couplet-physical-ai and remains vendor-neutral.

## Status

Provided as-is. This is an example integration, not a validated test-stand control system. No real stand validation is claimed.

## Install

Python 3.11 and [uv](https://docs.astral.sh/uv/) are needed for the demo and tests:

```sh
uv sync
uv run pytest
```

The sole runtime Python dependency is `pika`. No hardware or broker is needed for the offline demo.

## Quickstart: fake VI, no broker

```sh
uv run python examples/fake_vi.py
```

The script calls `exchange()` for three synthetic VI ticks, publishes fake temperature and voltage telemetry into an in-memory broker, prints each 5-tuple, and never actuates hardware. With no command policy configured, every tuple has an empty verb. To see an *opt-in proposed command* printed (still no actuation):

```sh
uv run python examples/fake_vi.py --policy examples/policies/lamp-stand.example.toml
```

`examples/send_command.py` and `examples/watch_telemetry.py` require a separately configured RabbitMQ broker; they are not needed for the offline quickstart. [CONTRACT.md](CONTRACT.md) specifies the wire format and Python Node signature.

## Configuration

`stand_bridge.py` reads existing environment variables first, then optional `.env` beside the module (ignored by git). The shipped `.env.example` is deliberately nonworking; replace its placeholders before using it. If no `.env` exists, the broker defaults to local RabbitMQ at `localhost:5672`, with RabbitMQ's localhost-only `guest`/`guest` account. `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`, and `RABBITMQ_PASS` override these values. `LV_AGENT_LOG` sets the log path (default `lv_agent.log`).

**No command policy is active by default.** Set `LV_COMMAND_POLICY` to the path of a reviewed TOML file to opt in; see [example policy](examples/policies/lamp-stand.example.toml). Each `[[commands]]` entry requires `verb`, `target`, `min_value`, `max_value`, `max_ttl_seconds`, and `min_interval_seconds`. A missing, unreadable, or malformed policy rejects all commands; policy is re-read on each validation, so removing it fails closed. The three synthetic lamps in the example policy are illustrative, not an approved deployment default. TTL must be a positive number, no more than both policy maximum and 300 seconds, with a timezone-aware, non-future issuance time. Numeric strings, booleans, non-finite or out-of-range values, and timestamps that overflow UTC are **rejected**, not silently clamped. Rate limits apply per target within the Python process and reset on process restart; configure hardware-side limits independently.

For a real broker, declare/bind the per-device command queue before sending commands if you must retain messages published before the first VI tick. The bridge creates the durable queue on its first poll. The `amq.topic` exchange is a RabbitMQ standard exchange. Telemetry and command acks are best-effort, not durable transaction confirmation.

## Prerequisites for real VI integration

NI LabVIEW, a compatible licensed Python Node, a configured Python 3.11 interpreter (such as `.venv/bin/python`), and any hardware drivers are separately supplied by the user. No NI runtime, VI, drivers, or SDKs are bundled. RabbitMQ is separately required for the live bridge but not the fake-VI demo. Configure the Python Node for module `stand_bridge`, function `exchange(headers, values, device_id, location)`; it returns `(verb, target, value, command_id, status)`.

## Safety and limits

**The Python layer is NOT a safety interlock.** LabVIEW and hardware retain final authority, including independent e-stop, limits, operator controls, and fault handling. A command is only a proposal to the VI; a `delivered` ack means handed to the VI, not applied to the device. A missing broker, invalid policy, or rejected command returns an empty verb; the VI must treat it as no action. The bridge may block on broker timeouts and does not provide real-time guarantees. Review the policy, VI wiring, and hardware interlocks independently before any live use. The offline demo does not validate a real stand.

## License

Original code is licensed under [Apache-2.0](LICENSE). See [NOTICE](NOTICE) for attribution.

## Contributing

See [CONTRIBUTING](https://github.com/couplet-physical-ai/.github/blob/main/CONTRIBUTING.md). Contributions require DCO sign-off (`git commit -s`); no CLA is required.

## Security

Report vulnerabilities via [GitHub private vulnerability reporting](https://github.com/couplet-physical-ai/labview-agent/security/advisories/new). See the [security policy](https://github.com/couplet-physical-ai/.github/blob/main/SECURITY.md); do not open a public issue.
