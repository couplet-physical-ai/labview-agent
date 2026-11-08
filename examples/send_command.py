"""Send one command to a stand and wait briefly for its ack.

Usage:
    python examples/send_command.py --device demo-stand --target lamp_3 --value 1
    python examples/send_command.py --device demo-stand --target lamp_1 --value 0
"""

import argparse
import datetime
import json
import sys
import uuid
from pathlib import Path

import pika

sys.path.insert(0, str(Path(__file__).parent.parent))
import stand_bridge


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--device", required=True, help="target device_id, e.g. demo-stand")
    parser.add_argument("--target", required=True, help="e.g. lamp_1, lamp_2, lamp_3")
    parser.add_argument("--value", type=float, required=True)
    parser.add_argument("--verb", default="set")
    parser.add_argument("--ttl", type=float, default=10.0, help="seconds until stale")
    parser.add_argument("--source", default="send_command.py")
    parser.add_argument("--ack-timeout", type=float, default=15.0)
    args = parser.parse_args()

    command_id = str(uuid.uuid4())
    payload = {
        "command_id": command_id,
        "issued_at": datetime.datetime.now(datetime.UTC).isoformat(),
        "ttl_seconds": args.ttl,
        "device_id": args.device,
        "verb": args.verb,
        "target": args.target,
        "value": args.value,
        "source": args.source,
    }

    params = pika.ConnectionParameters(
        host=stand_bridge._RABBITMQ_HOST,
        port=stand_bridge._RABBITMQ_PORT,
        credentials=pika.PlainCredentials(stand_bridge._RABBITMQ_USER, stand_bridge._RABBITMQ_PASS),
    )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # Subscribe to acks BEFORE publishing so we can't miss ours.
    ack_queue = channel.queue_declare(queue="", exclusive=True, auto_delete=True).method.queue
    channel.queue_bind(
        queue=ack_queue,
        exchange="amq.topic",
        routing_key=f"labview.cmd_ack.{stand_bridge._sanitize(args.device)}",
    )

    channel.basic_publish(
        exchange="amq.topic",
        routing_key=f"labview.cmd.{stand_bridge._sanitize(args.device)}",
        body=json.dumps(payload),
        properties=pika.BasicProperties(content_type="application/json"),
    )
    print(f"sent {command_id}: {args.verb} {args.target} = {args.value} (ttl {args.ttl}s)")

    deadline = datetime.datetime.now() + datetime.timedelta(seconds=args.ack_timeout)
    print(f"waiting up to {args.ack_timeout:.0f}s for ack (VI polls once per loop tick)...")
    while datetime.datetime.now() < deadline:
        method, _props, body = channel.basic_get(queue=ack_queue, auto_ack=True)
        if method is None:
            connection.sleep(0.25)
            continue
        ack = json.loads(body)
        if ack.get("command_id") == command_id:
            status = ack.get("status")
            reason = f" ({ack.get('reason')})" if ack.get("reason") else ""
            print(f"ack: {status}{reason}")
            break
    else:
        print("no ack (VI not running, or loop period longer than the timeout)")
    connection.close()


if __name__ == "__main__":
    main()
