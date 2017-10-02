#!/usr/bin/env python
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from uuid import uuid4
import time
import argparse

parser = argparse.ArgumentParser(description='Publishes Domain Query messages to Kafka')
parser.add_argument('--query', type=str)

args = parser.parse_args()

if args.query is None:
    query_string = 'SELECT * from global_temp.metrics LIMIT 5'
else:
    query_string = args.query

value_schema = avro.load('query.avsc')
key_schema = avro.load('query_id.avsc')
value = {"id": str(uuid4()), "query": query_string, "metadata": {"tenant_id": "acme_1", "timestamp": time.strftime("%c")}}
key = "query_producer1"
print('About to send message: {}'.format(value))

avroProducer = AvroProducer({'bootstrap.servers': 'kafka', 'schema.registry.url': 'http://localhost:8081'}, default_key_schema=key_schema, default_value_schema=value_schema)
#import pdb; pdb.set_trace()
avroProducer.produce(topic='query-in', value=value, key=key)
avroProducer.flush()
