#!/usr/bin/env python
from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError

c = AvroConsumer({'bootstrap.servers': 'kafka', 'group.id': 'groupid', 'schema.registry.url': 'http://127.0.0.1:8081'})
c.subscribe(['query-out'])
running = True
while running:
    try:
        msg = c.poll(10)
        if msg:
            if not msg.error():
                print(msg.value())
            elif msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
                running = False
    except SerializerError as e:
        print("Message deserialization failed for %s: %s" % (msg, e))
        running = False
        
c.close()