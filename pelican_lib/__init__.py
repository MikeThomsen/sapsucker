import avro
import pykafka
import pymysql
import psycopg2
import sys

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from io import BytesIO

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
			print("No kafka")
			print(self.profile)
			self.kafka_handler = None

	def finish(self):
		if self.kafka_handler:
			self.kafka_handler.finish()

	def show_stats(self, stats_table: dict):
		print("\n\n\n********************************")
		print("Processing statistics by query:")
		print("********************************\n\n\n")
		for table in stats_table:
			temp = stats_table[table]
			print(f"Query key: {table}")
			print(f"\tGood: {temp[GOOD_RECORDS]}")
			print(f"\tBad: {temp[BAD_RECORDS]}")
			print(f"\tTotal: {temp[TOTAL]}\n")

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
			schema_path = queries[query]['schema']
			self.cursor.execute(sql_query)
			colnames = [desc[0] for desc in self.cursor.description]
			print(colnames)
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
					content = self.avro_handler.make_record_set(schema_path, row_dicts)
					current_table[GOOD_RECORDS] = current_table[GOOD_RECORDS] + 1
					if self.kafka_handler:
						print("Producing...")
						self.kafka_handler.send_record_set(topic, content)
				except:
					import traceback
					traceback.print_exc()
					current_table[BAD_RECORDS] = current_table[BAD_RECORDS] + 1
					if self.command == "ingest":
						print("Fatal error processing records during ingest.")
						sys.exit(1)
				finally:
					current_table[TOTAL] = current_table[TOTAL] + 1
				rows = self.cursor.fetchmany(record_set_size)
		self.show_stats(stats_table)


class KafkaHandler:
	def __init__(self, profile: dict):
		print("Building KafkaHandler")
		self.profile = profile
		if 'broker_list' in self.profile['kafka']:
			broker_list = self.profile['kafka']['broker_list']
			hosts = ",".join(broker_list)
			print(hosts)
			self.client = pykafka.KafkaClient(hosts=hosts)
			self.producers = {}
		else:
			raise Exception("No broker_list branch under the kafka configuration.")

	def finish(self):
		for producer in self.producers:
			print(f"Stopping {producer}")
			self.producers[producer].stop()


	def send_record_set(self, topic: str, data: bytes):
		if topic not in self.producers:
			self.producers[topic] = self.client.topics[topic].get_sync_producer()
		self.producers[topic].produce(data)

