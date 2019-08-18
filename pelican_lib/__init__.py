import avro
import json
import pykafka
import pymysql
import psycopg2
import sys

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from io import BytesIO

import logging
import logging.handlers
import os
 
handler = logging.handlers.WatchedFileHandler(
    os.environ.get("LOGFILE", "./pelican.log"))
# formatter = logging.Formatter(logging.BASIC_FORMAT)
# handler.setFormatter(formatter)
root = logging.getLogger()
root.setLevel(os.environ.get("LOGLEVEL", "INFO"))
root.addHandler(handler)
root.addHandler(logging.StreamHandler())

def pelican_log(msg: str, ex: Exception = None):
	logging.info(msg)
	if ex:
		logging.exception(ex)


TOTAL = "total"
GOOD_RECORDS = "good"
BAD_RECORDS  = "bad"
DRY_RUN_RECORD_SET_SIZE = 1
INGEST_RECORD_SET_SIZE = 10

class AvroHandler:
	def __init__(self):
		self.schemas = {}

	def make_record_set(self, schema_path: str, items: list) -> bytes:
		if schema_path not in self.schemas:
			with open(schema_path, 'rb') as raw:
				self.schemas[schema_path] = avro.schema.Parse(raw.read())
		out = BytesIO()
		writer = DataFileWriter(out, DatumWriter(), self.schemas[schema_path])
		for item in items:
			writer.append(item)
		writer.flush()

		return out.getvalue()

class SqlRunner:
	def __init__(self, profile: dict, command: str):
		self.profile = profile
		self.command = command
		db = self.profile['database']
		if db["type"] == "postgres":
			self.connection = psycopg2.connect(host=db["host"], user=db["username"], password=db["password"], database=db["database_name"])
		elif db["type"] == "mysql":
			self.connection = pymysql.connect(host=db["host"], user=db["username"], password=db["password"], database=db["database_name"])
		else:
			raise Exception(f"Unsupported database type {db['type']}")
		self.cursor = self.connection.cursor()

		self.avro_handler = AvroHandler()

		if 'kafka' in self.profile and command == "ingest":
			self.kafka_handler = KafkaHandler(self.profile)
		else:
			pelican_log("No kafka")
			pelican_log(self.profile)
			self.kafka_handler = None

	def finish(self):
		if self.kafka_handler:
			self.kafka_handler.finish()

	def show_stats(self, stats_table: dict):
		pelican_log("\n\n\n********************************")
		pelican_log("Processing statistics by query:")
		pelican_log("********************************\n\n\n")
		for table in stats_table:
			temp = stats_table[table]
			pelican_log(f"Query key: {table}")
			pelican_log(f"\tGood: {temp[GOOD_RECORDS]}")
			pelican_log(f"\tBad: {temp[BAD_RECORDS]}")
			pelican_log(f"\tTotal: {temp[TOTAL]}\n")

	def execute_queries(self):
		queries = self.profile["queries"]

		stats_table = {}

		if self.command == "ingest":
			record_set_size = INGEST_RECORD_SET_SIZE
		else:
			record_set_size = DRY_RUN_RECORD_SET_SIZE

		for query in queries:
			sql_query   = queries[query]["sql"]
			topic       = queries[query]["kafka_topic"]
			schema_path = queries[query]['schema'] if "schema" in queries[query] else None
			self.cursor.execute(sql_query)
			colnames = [desc[0] for desc in self.cursor.description]
			current_table = {
				GOOD_RECORDS: 0,
				BAD_RECORDS: 0,
				TOTAL: 0
			}

			stats_table[query] = current_table

			rows = self.cursor.fetchmany(record_set_size)
			while rows:
				row_dicts = [ dict(zip(colnames, row)) for row in rows ]

				try:
					if schema_path:
						content = self.avro_handler.make_record_set(schema_path, row_dicts)
					else:
						content = json.dumps(row_dicts).encode('utf-8')
					current_table[GOOD_RECORDS] = current_table[GOOD_RECORDS] + 1
					if self.kafka_handler:
						self.kafka_handler.send_record_set(topic, content)
				except:
					import traceback
					traceback.pelican_log_exc()
					current_table[BAD_RECORDS] = current_table[BAD_RECORDS] + 1
					if self.command == "ingest":
						pelican_log("Fatal error processing records during ingest.")
						sys.exit(1)
				finally:
					current_table[TOTAL] = current_table[TOTAL] + 1
				rows = self.cursor.fetchmany(record_set_size)
		self.show_stats(stats_table)


class KafkaHandler:
	def __init__(self, profile: dict):
		pelican_log("Building KafkaHandler")
		self.profile = profile
		if 'broker_list' in self.profile['kafka']:
			broker_list = self.profile['kafka']['broker_list']
			hosts = ",".join(broker_list)
			pelican_log(hosts)
			self.client = pykafka.KafkaClient(hosts=hosts)
			self.producers = {}
		else:
			raise Exception("No broker_list branch under the kafka configuration.")

	def finish(self):
		for producer in self.producers:
			pelican_log(f"Stopping {producer}")
			self.producers[producer].stop()


	def send_record_set(self, topic: str, data: bytes):
		if topic not in self.producers:
			self.producers[topic] = self.client.topics[topic].get_sync_producer()
		self.producers[topic].produce(data)

