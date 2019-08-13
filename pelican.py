#!/usr/local/bin/python3.7
import argparse
import docker
import json
import yaml
import os
import os.path

from pelican import SqlRunner

def parse_args():
	parser = argparse.ArgumentParser(description='Build docker images around data for quick extraction.')
	parser.add_argument('command', choices=["build", "dryrun", "showprofiles", "ingest", "makedockerfile"])
	parser.add_argument("--profile", type=str)
	parser.add_argument("--pelicanfile", type=str)

	return parser.parse_args()

def create_dockerfile(profile: dict):
	from_line = f"FROM {profile['database']['type']}:{profile['database']['version']}"
	copy_line = f"COPY {profile['database']['source']} /docker-entrypoint-initdb.d"

	docker_path = f"./Dockerfile.{profile['profile']}.{profile['database']['type']}"
	with open(docker_path, "w", encoding='utf-8') as output:
		output.write(f"{from_line}\n")
		output.write(f"{copy_line}\n")
		output.write("RUN apt-get update\n")
		output.write("RUN apt-get -y upgrade\n")
		output.write("RUN apt-get install -y wget build-essential python-dev python-setuptools python-pip python-smbus\n")
		output.write("RUN apt-get install -y libncursesw5-dev libgdbm-dev libc6-dev\n")
		output.write("RUN apt-get install -y zlib1g-dev libsqlite3-dev tk-dev\n")
		output.write("RUN apt-get install -y libssl-dev openssl\n")
		output.write("RUN apt-get install -y libffi-dev\n")
		output.write("RUN wget https://www.python.org/ftp/python/3.7.2/Python-3.7.2.tgz -O /tmp/python.tgz\n")
		output.write("RUN cd /tmp && tar -zxvf python.tgz && cd Python-3.7.2 && ./configure --enable-optimizations && make altinstall\n")
		output.write("RUN pip3.7 install pykafka pymysql psycopg2-binary avro-python3 docker pyyaml\n")
		output.write("RUN mkdir /opt/pelican\n")
		output.write("COPY pelican.py /opt/pelican\n")
		output.write("ADD pelican /opt/pelican/pelican\n")
		output.write("RUN chmod 755 /opt/pelican/pelican.py\n")

		for key in profile["queries"]:
			query = profile["queries"][key]
			avro_path = os.path.dirname(query["schema"])
			output.write(f"COPY {query['schema']} /opt/pelican/{query['schema']}\n")

		#output.write("COPY user.avsc /opt/pelican/user.avsc\n")
		output.write("COPY Pelicanfile.postgres /opt/pelican/Pelicanfile")
		print("Wrote dockerfile")

	return docker_path


if __name__ == "__main__":
	docker_client = docker.from_env()
	args = parse_args()
	configuration_path = args.pelicanfile if args.pelicanfile else "./Pelicanfile"
	profiles = {}
	for profile in yaml.load_all(open(configuration_path), Loader=yaml.SafeLoader):
		if "profile" not in profile:
			raise KeyError("Missing key 'profile.'")
		profiles[profile['profile']] = profile
	
	if args.command == "showprofiles":
		print("Available profiles:")
		for profile in profiles:
			print(f"{profile}:\n{json.dumps(profiles[profile], sort_keys=True, indent=4)}")
	elif args.command in [ "build", "makedockerfile"]:
		print(f"Using profile {args.profile}:\n{json.dumps(profiles[args.profile], indent=4, sort_keys=True)}")

		current_profile = profiles[args.profile]
		ret_val = create_dockerfile(current_profile)

		if args.command == "build":
			print("Building docker image (this could take a while)...")
			docker_client.images.build(path="./", dockerfile=ret_val, tag=profile["docker_tag"])
			print("Finished building docker image.")
	elif args.command in [ "dryrun", "ingest" ]:
		print(f"Running a \"{args.command}\"")
		current_profile = profiles[args.profile]
		sql_runner = SqlRunner(current_profile, args.command)
		sql_runner.execute_queries()
		sql_runner.finish()

