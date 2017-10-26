#!/usr/bin/env python
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from uuid import uuid4
import time
import argparse

parser = argparse.ArgumentParser(description='Publishes Domain Query messages to Kafka')
parser.add_argument('--query', type=str)
parser.add_argument('--tenant', type=str)
args = parser.parse_args()

if args.query is None:
    query_string = 'computer'
else:
    query_string = args.query


if args.tenant is None:
    tenant = 'anyone'
else:
    tenant = args.tenant

value_schema = avro.load('query2.avsc')
key_schema = avro.load('query_id.avsc')
now_in_secs = time.time()
one_hr_ago_in_secs = now_in_secs - 60*60;
start_ts = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(one_hr_ago_in_secs))
end_ts = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(now_in_secs))
value = {"id": str(uuid4()), "query": query_string, "metadata": {"timestamp": str(int(time.time())),"tenant_id": tenant}, "querytype": "TYPED", "queryParams": {"query": query_string, "tenant": tenant, "start": "2017-10-23T00:00:00.000Z", "end": end_ts, "sortby": "CPU", "sortorder": "ASC"}}

key = "query_producer3"

avroProducer = AvroProducer({'bootstrap.servers': 'kafka', 'schema.registry.url': 'http://localhost:8081'}, default_key_schema=key_schema, default_value_schema=value_schema)
#import pdb; pdb.set_trace()
msg = avroProducer.produce(topic='query-in', value=value, key=key)
print(msg)
avroProducer.flush()
