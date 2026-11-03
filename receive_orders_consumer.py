import json
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "127.0.0.1:9092",
    "group.id": "receive_orders",
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["orders"])

orders_received = set()

while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        continue
    order = json.loads(msg.value())
    if order["msg_id"] in orders_received:
        print("duplicate order skipping", order["msg_id"], order["customer"], order["item"])
        continue
    orders_received.add(order["msg_id"])
    print(order)


