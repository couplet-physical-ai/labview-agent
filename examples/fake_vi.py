"""Offline fake-VI loop and in-memory broker. No LabVIEW, hardware, or RabbitMQ needed.

Run: uv run python examples/fake_vi.py
Opt in: uv run python examples/fake_vi.py \
    --policy examples/policies/lamp-stand.example.toml
The returned command is PRINTED ONLY; this demo never actuates anything.
"""

import argparse
import datetime
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import stand_bridge as bridge


class FakeBroker:
    def __init__(self):
        self.pending = []
        self.published = []
        self.delivery_tag = 0

    def queue_declare(self, **_kwargs):
        pass

    def queue_bind(self, **_kwargs):
        pass

    def basic_publish(self, *, exchange, routing_key, body, properties):
        self.published.append((routing_key, json.loads(body)))

    def basic_get(self, **_kwargs):
        if not self.pending:
            return None, None, None
        self.delivery_tag += 1
        method = type("Delivery", (), {"delivery_tag": self.delivery_tag})()
        return method, None, self.pending.pop(0)

    def basic_ack(self, _tag):
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy", type=Path, help="opt in to a TOML command policy")
    parser.add_argument("--ticks", type=int, default=3)
    args = parser.parse_args()
    broker = FakeBroker()
    bridge._get_channel = lambda: broker
    bridge._POLICY_PATH = str(args.policy) if args.policy else ""
    bridge._declared_queues.clear()
    if args.policy:
        broker.pending.append(json.dumps({
            "command_id": "demo-1", "device_id": "demo-stand", "verb": "set",
            "target": "lamp_1", "value": 0.0, "ttl_seconds": 5,
            "issued_at": datetime.datetime.now(datetime.UTC).isoformat(),
        }).encode())
    for tick in range(args.ticks):
        # Exactly the callable signature used by a LabVIEW Python Node, with synthetic input.
        result = bridge.exchange("Temperature( C);Voltage( V)",
                                 [22.0 + tick, 12.0], "demo-stand", "simulation")
        print(f"tick {tick}: {result}")
    print(f"broker events: {len(broker.published)}; simulated only, no hardware output")


if __name__ == "__main__":
    main()
