"""Live-print telemetry from the stand(s).

Usage:
    python examples/watch_telemetry.py                     # everything
    python examples/watch_telemetry.py --key 'labview.Lamp_1_Voltage'
    python examples/watch_telemetry.py --key 'labview.cmd_ack.demo-stand'
"""

import argparse
import json
import sys
from pathlib import Path

import pika

sys.path.insert(0, str(Path(__file__).parent.parent))
import stand_bridge


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--key", default="labview.#", help="topic binding key")
    args = parser.parse_args()

    params = pika.ConnectionParameters(
        host=stand_bridge._RABBITMQ_HOST,
        port=stand_bridge._RABBITMQ_PORT,
        credentials=pika.PlainCredentials(stand_bridge._RABBITMQ_USER, stand_bridge._RABBITMQ_PASS),
    )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    queue = channel.queue_declare(queue="", exclusive=True, auto_delete=True).method.queue
    channel.queue_bind(queue=queue, exchange="amq.topic", routing_key=args.key)
    print(f"bound to amq.topic with key {args.key!r} — Ctrl-C to stop")

    def on_message(_ch, method, _props, body) -> None:
        try:
            msg = json.loads(body)
            print(f"[{method.routing_key}] {json.dumps(msg)}")
        except json.JSONDecodeError:
            print(f"[{method.routing_key}] {body!r}")

    channel.basic_consume(queue=queue, on_message_callback=on_message, auto_ack=True)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        connection.close()


if __name__ == "__main__":
    main()
