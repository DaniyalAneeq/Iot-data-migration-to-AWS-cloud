import json

with open("rsa_key.p8") as f:
    lines = f.read().splitlines()

key_body = "".join(
    line for line in lines
    if "BEGIN PRIVATE KEY" not in line and "END PRIVATE KEY" not in line
)

config = {
    "name": "snowflake-cdc-sink",
    "config": {
        "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
        "tasks.max": "1",
        "topics": "cdc.public.iot_events",
        "snowflake.topic2table.map": "cdc.public.iot_events:IOT_EVENTS",
        "buffer.count.records": "100",
        "buffer.flush.time": "10",
        "buffer.size.bytes": "5000000",
        "snowflake.url.name": "https://TGEGCPO-CZB06382.snowflakecomputing.com:443",
        "snowflake.user.name": "KAFKA_CONNECTOR_USER",
        "snowflake.private.key": key_body,
        "snowflake.database.name": "HACKATHON_IOT",
        "snowflake.schema.name": "RAW",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "com.snowflake.kafka.connector.records.SnowflakeJsonConverter",
    },
}

with open("snowflake_connector_config.json", "w") as f:
    json.dump(config, f)

print("Wrote snowflake_connector_config.json")
