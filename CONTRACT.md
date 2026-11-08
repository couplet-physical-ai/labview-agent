# Agent ↔ VI data contract

The wire contract for a LabVIEW VI calling `stand_bridge.py` once per tick. This bridge proposes commands but is **not** a safety interlock; the VI and hardware retain final control. No live stand has been validated with this example.

## Broker

RabbitMQ `amq.topic` exchange, local `localhost:5672` and `guest`/`guest` by default, with `RABBITMQ_HOST`, `RABBITMQ_PORT`, `RABBITMQ_USER`, and `RABBITMQ_PASS` environment overrides. The fake-VI demo uses only an in-memory broker.

## Telemetry: VI → broker

One JSON message per parseable header per VI loop tick, routing key `labview.<measurement>` (non-alphanumerics replaced by `_`). Consumers bind their own queues to `labview.#` or a narrower key.

```json
{
  "timestamp": "2026-07-17T15:22:52.888654+00:00",
  "measurement": "Temperature",
  "value": 22.0,
  "unit": "C",
  "device_id": "demo-stand",
  "location": "simulation",
  "source": "labview"
}
```

## Commands: broker → VI

Publish JSON to `amq.topic` with routing key `labview.cmd.<device_id>`. On its first poll, the bridge creates and binds the durable `labview_commands.<device_id>` queue. At most one accepted command is returned per tick (up to ten queued messages are inspected per tick).

```json
{
  "command_id": "example-1",
  "issued_at": "2026-07-17T16:00:00Z",
  "ttl_seconds": 5,
  "device_id": "demo-stand",
  "verb": "set",
  "target": "lamp_1",
  "value": 0.0,
  "source": "example"
}
```

No configured `LV_COMMAND_POLICY` means **no commands are returned**. An explicitly configured TOML file must contain `[[commands]]` entries with `verb`, `target`, `min_value`, `max_value`, `max_ttl_seconds`, and `min_interval_seconds`; see [the example](examples/policies/lamp-stand.example.toml). Any malformed or missing policy rejects all commands. Unlisted verb/target pairs, non-numeric or out-of-bounds values (including numeric strings and booleans), timestamps without timezones or from the future or outside the supported UTC range, stale or excessive TTL, and rate-limited commands are rejected. The policy is a software filter, not an interlock. Limits are per process and reset on restart.

Messages are broker-acked on receipt (at-most-once toward the VI). The bridge best-effort publishes a receipt on `labview.cmd_ack.<device_id>` containing `command_id`, `device_id`, `status` (`delivered` or `rejected`), `reason`, and UTC `timestamp`. `delivered` means handed to the VI, not applied to hardware; verify device state separately.

## LabVIEW boundary

```text
exchange(headers, values, device_id, location)
    -> (verb, target, value, command_id, status)
```

`headers` is a list or semicolon-separated string of `Name( unit )` entries; `values` is an equal-length numeric list. Unitless headers are skipped. The 5-tuple maps to `(string, string, DBL, string, string)`. `verb == ""` means no command, including on broker failure; the VI must do nothing. Nonempty verbs are only proposals and still require locally enforced hardware/VI limits, e-stop, and operator arbitration. `status` is a diagnostic string, not a control signal. `publish_only(...)` emits telemetry and never polls commands during initial VI wire-up.
