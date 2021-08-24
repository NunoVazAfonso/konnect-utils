"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"

# single connector
CONNECTOR_NAME = "test1-stations-jdbc"
CONNECTION_URL = "jdbc:postgresql://framework-kafka_postgres_1:5432/cta"


# multiple connectors 
connectors = [
	# {
	# 	"type" : "jdbc",
	# 	"name": "stations-jdbc",
	# 	"url": "jdbc:postgresql://framework-kafka_postgres_1:5432/cta",
	# 	"user": "cta_admin",
	# 	"password": "chicago",
	# 	"table": "stations",
	# 	"incrementing_column": "stop_id",
	# 	"topic_prefix": "com.nva.pg.0709.",
	# 	"poll_interval": "5000",
	# 	"max_rows": "500"
	# },
	# {
	# 	"type" : "mysql",
	# 	"name": "mysql-jdbc",
	# 	"url": "jdbc:mysql://framework-kafka_mysql_1:3306/default_schema",
	# 	"user": "cta_admin",
	# 	"password": "chicago",
	# 	"table": "test_table",
	# 	"incrementing_column": "id",
	# 	"ts_column": "updated_at",
	# 	"topic_prefix": "com.nva.mysql_test.",
	# 	"poll_interval": "5000",
	# 	"max_rows": "500"
	# },
	# {
	# 	"type" : "file",
	# 	"name": "test-file1",
	# 	"topic": "com.nva.file.0709.1",
	# 	"poll_interval": "5000",
	# 	"max_rows": "500",
	# 	"tasks.max":"1",
	# 	"file":"/tmp/test.txt"
	# },
	# {
	# 	"type" : "file",
	# 	"name": "test-file2",
	# 	"topic": "com.nva.file.0709.2",
	# 	"poll_interval": "5000",
	# 	"max_rows": "500",
	# 	"tasks.max":"1",
	# 	"file":"/tmp/test6.txt"
	# },
	{
		"type" : "mysql-deb",
		"name": "mysql-debezium",
		"url": "framework-kafka_mysql_1",
		"schema_name": "default_schema",
		"user": "cta_admin",
		"password": "chicago",
		"table": "test_table",
		"topic_prefix": "com_nva_debezium_test_",
		"poll_interval": "5000",
		"max_rows": "500"
	},
]

def get_connector_configs( connector_info ):

	if connector_info["type"] == "mysql": 
		connector_config = {
			"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
			"tasks.max":1,
			"connection.url": connector_info["url"],
			"connection.user": connector_info["user"],
			"connection.password": connector_info["password"],
			"table.whitelist": connector_info["table"],
			"mode": "timestamp+incrementing",
			"timestamp.column.name": connector_info["ts_column"],
			"incrementing.column.name": connector_info["incrementing_column"],
			"topic.prefix": connector_info["topic_prefix"]
		}

		return connector_config

	elif connector_info["type"] == "mysql-deb": 
		connector_config = {
        	"connector.class": "io.debezium.connector.mysql.MySqlConnector", 
        	"database.hostname": connector_info["url"], 
        	"database.user": connector_info["user"], 
        	"database.password": connector_info["password"], 
        	"database.server.name": connector_info["topic_prefix"], 
        	"database.include.list": connector_info["schema_name"], 
	        "database.history.kafka.bootstrap.servers": "kafka0:19092", 
        	"database.history.kafka.topic": "debeziumhistory_" +connector_info["topic_prefix"], 
        	"include.schema.changes": "true" 
		}

		return connector_config

	elif connector_info["type"] == "jdbc": 
		connector_config = {
			"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
			"key.converter": "org.apache.kafka.connect.json.JsonConverter",
			"key.converter.schemas.enable": "true",
			"value.converter": "org.apache.kafka.connect.json.JsonConverter",
			"value.converter.schemas.enable": "true",
			"batch.max.rows": connector_info["max_rows"],
			"connection.url": connector_info["url"],
			"connection.user": connector_info["user"],
			"connection.password": connector_info["password"],
			"table.whitelist": connector_info["table"],
			"mode": "incrementing",
			"incrementing.column.name": connector_info["incrementing_column"],
			"topic.prefix": connector_info["topic_prefix"],
			"poll.interval.ms": connector_info["poll_interval"],
		}

		return connector_config

	elif connector_info["type"] == "file": 
		connector_config = {
			"connector.class": "FileStreamSource",
			"key.converter": "org.apache.kafka.connect.json.JsonConverter",
			"key.converter.schemas.enable": "true",
			"value.converter": "org.apache.kafka.connect.json.JsonConverter",
			"value.converter.schemas.enable": "true",
			"tasks.max": connector_info["tasks.max"],
			"topic": connector_info["topic"],
			"file": connector_info["file"],
		}

		return connector_config

	else : 
		print("Unrecognized connector type. Only \"jdbc\" available.")
		logging.debug("Unrecognized connector type. Only \"jdbc\" available.")
		return 0


def configure_connector( connector_info ):
	"""Starts and configures the Kafka Connect connector"""
	logging.debug(f"creating or updating kafka connect connector {connector_info['name']}...")


	resp = requests.get(f"{KAFKA_CONNECT_URL}/{connector_info['name']}")
	if resp.status_code == 200:
		logging.debug("connector already created skipping recreation")
		print(resp)
		return

	resp = requests.post(
		KAFKA_CONNECT_URL,
		headers={"Content-Type": "application/json"},
		data=json.dumps({
			"name": connector_info["name"],
			"config": get_connector_configs( connector_info )
		}),
	)

	## Ensure a healthy response was given
	print(resp.__dict__)

	resp.raise_for_status()
	

	logging.debug("connector created successfully")


if __name__ == "__main__":
	for connector in connectors:       
		print(connector['name'])
		configure_connector(connector)
