help:
	@echo "build - The Docker image kumarroshandot/spark-postgres"
	@echo "spark-postgres-up - Run a Spark cluster (exposes port 8100) and Postgres Service (Port :5432), pgadmin (port: 8888)"
	@echo "stop-all-services- Stop the services of the container"
	@echo "wipeout-everything- Stop and Remove all the containers"
	@echo "run-etl-jobs- Run ETL Jobs and load data from json to datalake in postgres"


all: default spark-postgres-up

default: build

build:
	docker build -t kumarroshandot/spark-postgres -f docker/Dockerfile .

spark-postgres-up:
	docker-compose -f docker/docker-compose.yml up -d

stop-all-services:
	docker-compose -f docker/docker-compose.yml stop

wipeout-everything:
	docker-compose -f docker/docker-compose.yml down

run-etl-jobs:
	cp src/spark/* /tmp/
	cp -r source_data /tmp/
	docker exec spark spark-submit --master spark://spark:7077 /data/etl_rawlayer.py --source_file_path '/data/source_data/jsonfiles'
	docker exec spark spark-submit --master spark://spark:7077 /data/etl_cleanlayer.py
	docker exec spark spark-submit --master spark://spark:7077 /data/etl_agglayer.py