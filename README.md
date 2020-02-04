To run the application with default main class from a terminal: ``java -jar kafka-client.jar``

To run the application with specified main class from a terminal: ``java -cp kafka-client.jar com.ncr.kafka.client.KafkaConsumerApp``

To run word count class:

* Create input topic: ``kafka-topics --create --topic streams-plaintext-input --zookeeper localhost:2181 --partitions 1 --replication-factor 1``
* Create output topic: ``kafka-topics --create --topic streams-wordcount-output --zookeeper localhost:2181 --partitions 1 --replication-factor 1``
* Use Kafka producer: ``kafka-console-producer --broker-list localhost:9092 --topic streams-plaintext-input``
* Use Kafka producer with file: ``cat data_payload_one_record.json | kafka-console-producer --broker-list localhost:9092 --topic streams-plaintext-input``
* Use Kafka consumer: ``kafka-console-consumer --topic streams-wordcount-output --from-beginning --bootstrap-server localhost:9092 --property print.key=true  --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer``

To run metadata class:

* Create input topic: ``kafka-topics --create --topic streams-metadata-input --zookeeper localhost:2181 --partitions 1 --replication-factor 1``
* Create output topic: ``kafka-topics --create --topic streams-metadata-output --zookeeper localhost:2181 --partitions 1 --replication-factor 1``
* Use Kafka producer: ``kafka-console-producer --broker-list localhost:9092 --topic streams-metadata-input``
* Use Kafka producer with key: ``kafka-console-producer --broker-list localhost:9092 --topic streams-metadata-input --property "parse.key=true" --property "key.separator=:"``
* Use Kafka producer with file: ``cat data_payload_one_record.json | kafka-console-producer --broker-list localhost:9092 --topic streams-metadata-input``
* Use Kafka consumer: ``kafka-console-consumer --topic streams-metadata-output --from-beginning --bootstrap-server localhost:9092 --property print.key=true  --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer``