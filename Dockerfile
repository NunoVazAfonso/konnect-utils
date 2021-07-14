FROM confluentinc/cp-kafka-connect:5.2.2

RUN confluent-hub install --no-prompt wepay/kafka-connect-bigquery:1.1.0
