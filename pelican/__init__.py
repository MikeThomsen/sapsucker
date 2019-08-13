import avro
import pykafka
import pymysql
import psycopg2

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from io import BytesIO

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

		if 'kafka' in self.profile:
			self.kafka_handler = KafkaHandler(self.profile)
		else:
			print("No kafka")
			print(self.profile)
			self.kafka_handler = None

	def finish(self):
		if self.kafka_handler:
			self.kafka_handler.finish()

	def execute_queries(self):
		queries = self.profile["queries"]
		for query in queries:
			sql_query   = queries[query]["sql"]
			topic       = queries[query]["kafka_topic"]
			schema_path = queries[query]['schema']
			self.cursor.execute(sql_query)
			colnames = [desc[0] for desc in self.cursor.description]
			print(colnames)

			rows = self.cursor.fetchmany(2)
			while rows:
				if self.kafka_handler:
					print("Producing!")
					row_dicts = [ dict(zip(colnames, row)) for row in rows ]
					content = self.kafka_handler.make_record_set(schema_path, row_dicts)
					self.kafka_handler.send_record_set(topic, content)
				rows = self.cursor.fetchmany(2)



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
			self.schemas = {}
		else:
			raise Exception("No broker_list branch under the kafka configuration.")

	def finish(self):
		for producer in self.producers:
			print(f"Stopping {producer}")
			self.producers[producer].stop()

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


	def send_record_set(self, topic: str, data: bytes):
		if topic not in self.producers:
			self.producers[topic] = self.client.topics[topic].get_sync_producer()
		self.producers[topic].produce(data)

