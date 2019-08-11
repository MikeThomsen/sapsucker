import pykafka
import pymysql
import psycopg2

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

	def execute_queries(self):
		queries = self.profile["queries"]
		for query in queries:
			sql_query = queries[query]["sql"]
			topic      = queries[query]["kafka_topic"]
			self.cursor.execute(sql_query)

			row = self.cursor.fetchmany(2)
			while row:
				print(row)
				row = self.cursor.fetchmany(2)

