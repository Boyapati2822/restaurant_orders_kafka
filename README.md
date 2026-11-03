# restaurant_orders_kafka

Small practice project to learn Kafka basics. One app sends restaurant orders to a Kafka topic, 
another app reads them and makes sure the same order isn't processed twice.

## Files

- `orders_producer.py` - sends an order to the `orders` topic
- `receive_orders_consumer.py` - reads orders from `orders` and skips duplicates

## Setup

I'm running Kafka in Docker (no UI), using Colima on Mac.

```
colima start
docker run -d --name kafka -p 9092:9092 apache/kafka:4.4.0
```

Create the topic (5 partitions):

```
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic orders --partitions 5 --bootstrap-server localhost:9092
```

Python env:

```
python3 -m venv .venv
source .venv/bin/activate
pip install confluent-kafka
```

## Running it

Start the consumer first in one terminal:

```
python receive_orders_consumer.py
```

Then send an order from another terminal:

```
python orders_producer.py
```

## What an order looks like

```
{
  "msg_id": "2a677940-...",
  "order_ID": "ORD123",
  "customer": "whisky",
  "item": "Biryani",
  "ordered_at": "2026-11-03T20:10:00"
}
```

`order_ID` is used as the key, so messages for the same order always go to the same partition.

## Handling duplicates

Kafka can end up with the same message twice (producer retry, consumer crash before commit etc). The duplicate gets a new offset but the `msg_id` stays the same, so I check `msg_id`, not the offset.

The consumer keeps a set of msg_ids it has already seen. If a msg_id is already in the set it prints "duplicate order skipping" and moves on, otherwise it adds it and processes the order.

To test it, the producer sends the same order twice on purpose. The consumer prints the order once and skips the second one.

## Notes

- Use `127.0.0.1:9092` instead of `localhost:9092`. On my Mac, localhost tried IPv6 first and kept printing connection errors.
- `auto.offset.reset` is set to `earliest` so a new consumer group reads from the start of the topic and doesn't miss anything.
- Group id is `receive_orders`. If I run the consumer in 2 terminals with the same group id, Kafka splits the 5 partitions between them.

## TODO

- The seen msg_ids are only in memory, so they're gone after a restart. Need to save them somewhere (sqlite / redis).
