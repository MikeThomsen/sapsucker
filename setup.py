try:
    from setuptools import setup
except:
    from distutils.core import setup

config = {
    'description': 'Pelican is a tool for dockerizing databases and making them ready for quick ingestion.',
    'author': 'Mike Thomsen',
    'url': 'No URL',
    'download_url': 'Just local',
    'author_email': 'mikerthomsen@gmail.com',
    'version': '0.5',
    'install_requires': [ 'pykafka', 'pymysql', 'pyyaml', 'psycopg2-binary', 'avro-python3', 'docker'],
    'packages': ['pelican_lib'],
    'scripts': ['bin/pelican'],
    'name': 'pelican'
}

setup(**config)
