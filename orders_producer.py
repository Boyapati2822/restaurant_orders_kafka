import uuid
import json
from datetime import datetime
from confluent_kafka import Producer



producer = Producer({"bootstrap.servers" : "127.0.0.1:9092"})

event = {"msg_id": str(uuid.uuid4()), "order_ID": "ORD123", "customer": "whisky", "item": "Biryani", "ordered_at": datetime.now().isoformat()}

producer.produce("orders", key=event["order_ID"], value = json.dumps(event))
producer.produce("orders", key=event["order_ID"], value = json.dumps(event))
producer.flush()
print("sent", event)