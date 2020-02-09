To run the application with default main class from a terminal: ``java -jar kafka-client.jar``

To run the application with specified main class from a terminal: ``java -cp kafka-client.jar com.ncr.kafka.client.KafkaConsumerApp``

To run word count class:

* Create input topic: ``kafka-topics --create --topic streams-plaintext-input --zookeeper localhost:2181 --partitions 1 --replication-factor 1``
* Create output topic: ``kafka-topics --create --topic streams-wordcount-output --zookeeper localhost:2181 --partitions 1 --replication-factor 1``
* Use Kafka producer: ``kafka-console-producer --broker-list localhost:9092 --topic streams-plaintext-input``
* Use Kafka producer with file: ``cat data_payload_one_record.json | kafka-console-producer --broker-list localhost:9092 --topic streams-plaintext-input``
* Use Kafka consumer: ``kafka-console-consumer --topic streams-wordcount-output --from-beginning --bootstrap-server localhost:9092 --property print.key=true  --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer``

To run metadata class:

* Create input topic: ``kafka-topics --create --topic kafka-streams-input --zookeeper localhost:2181 --partitions 1 --replication-factor 1``
* Create output topic: ``kafka-topics --create --topic kafka-streams-output --zookeeper localhost:2181 --partitions 1 --replication-factor 1``
* Use Kafka producer: ``kafka-console-producer --broker-list localhost:9092 --topic kafka-streams-input``
* Use Kafka producer with key: ``kafka-console-producer --broker-list localhost:9092 --topic kafka-streams-input --property "parse.key=true" --property "key.separator=:"``
* Use Kafka producer with file: ``cat data_payload_one_record.json | kafka-console-producer --broker-list localhost:9092 --topic kafka-streams-input``
* Use Kafka consumer: ``kafka-console-consumer --topic kafka-streams-output --from-beginning --bootstrap-server localhost:9092 --property print.key=true  --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer``


* Send and receive JSON objects with Kafka java client: https://medium.com/@asce4s/send-and-receive-json-objects-with-kafka-java-client-41bfbb4de108
* Error handling: https://www.confluent.io/blog/spring-for-apache-kafka-deep-dive-part-1-error-handling-message-conversion-transaction-support/
* Common error handling patterns: https://docs.confluent.io/current/streams/faq.html
& Kafka topology for optimization: https://www.confluent.io/blog/optimizing-kafka-streams-applications/
* Online tool to display the topology graphically: https://zz85.github.io/kafka-streams-viz/
* Log Compact setting: https://stackoverflow.com/questions/53216633/kafka-compacted-topic-without-limited-retention


* Consumer isolation level for atomic transaction: isolation.level: read_committed
* To enable optimizations using topology: StreamsConfig.TOPOLOGY_OPTIMIZATION = StreamsConfig.OPTIMIZE
* Enable log compaction: log.cleanup.policy=compact
* Records won’t get compacted until after this period: log.cleaner.min.compaction.lag.ms = 1000