"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"

# you need to pass SCHEMA INFORMATION in order for sink to work
# multiple connectors 
connector_sink = [
    #{
    #    "type" : "jdbc",
    #    "name": "stations-sink",
    #    "source_topic": "com.nva.pg.0709.stations",
    #    "url": "jdbc:postgresql://framework-kafka_postgres-sink_1:5432/cta",
    #    "user": "cta_admin",
    #    "password": "chicago",
    #    "table": "stations",
    #    "incrementing_column": "stop_id",
    #    "topic_prefix": "com.nva.pg.0709.",
    #    "poll_interval": "5000",
    #    "max_rows": "500"
    #},
    {
        "type": "bq" ,
        "name": "bq-sink",
        "source_topic": "com.nva.mysql_test.test_table",
        "destination_project": "vernal-citadel-318510",
        "key_location": "/tmp/key.json",
        "destination_dataset": ".*=kafkatest",
    }

]

def get_connector_configs( connector_info ):

    if connector_info["type"] == "jdbc": 
        connector_config = {
            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "true",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "true",
            "tasks.max": "1",
            "auto.create": "true",
            "auto.evolve": "true",
            "connection.url": connector_info["url"],
            "connection.user": connector_info["user"],
            "connection.password": connector_info["password"],
            "insert.mode": "insert",
            "pk.mode": "none",
            "table.name.format": "kafka_stations-sink", 
            "topics": connector_info["source_topic"]  
        }

        return connector_config

    elif connector_info["type"] == "bq":
        connector_config = {
            "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
            "tasks.max" : "1",
            "topics" : connector_info["source_topic"],
            "sanitizeTopics" : "true",
            "autoCreateTables" : "true",
            "autoUpdateSchemas" : "true",
            "schemaRetriever" : "com.wepay.kafka.connect.bigquery.retrieve.IdentitySchemaRetriever",
            "project" : connector_info["destination_project"],
            "defaultDataset": "kafkatest",
            "keyfile" : connector_info["key_location"],
            "transforms": "addTS",
            "transforms.addTS.type": "org.apache.kafka.connect.transforms.InsertField$Value",
            "transforms.addTS.timestamp.field": "op_ts",
            "allowNewBigQueryFields" :"true",
            "allowBigQueryRequiredFieldRelaxation" :"true",
        }

        return connector_config

    else : 
        print("Unrecognized connector type. Only \"jdbc\" available.")
        logging.debug("Unrecognized connector type. Only \"jdbc\" available.")
        return 0


def configure_connector( connector_info ):
    """Starts and configures the Kafka Connect connector"""
    logging.debug(f"Creating or updating kafka connect sink connector {connector_info['name']}...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{connector_info['name']}")
    if resp.status_code == 200:
        print("connector already created skipping recreation")
        return

    print( f"Trying to create new Sink connector {connector_info['name']}" )

    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "name": connector_info["name"],
            "config": get_connector_configs( connector_info )
        }),
    )

    ## Ensure a healthy response was given
    print("oi")
    print(resp.text)
    resp.raise_for_status()
    

    logging.debug("Sink connector created successfully")


if __name__ == "__main__":
    for connector in connector_sink:       
        print(connector['name'])
        configure_connector(connector)
