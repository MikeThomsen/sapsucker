import argparse
import yaml

def parse_args():
	parser = argparse.ArgumentParser(description='Build docker images around data for quick extraction.')
	parser.add_argument('command', choices=["build", "dryrun", "showprofiles"])
	parser.add_argument("--pelicanfile", type=str)

	return parser.parse_args()


if __name__ == "__main__":
	args = parse_args()
	configuration_path = args.pelicanfile if args.pelicanfile else "./Pelicanfile"
	loaded = yaml.load_all(open(configuration_path))