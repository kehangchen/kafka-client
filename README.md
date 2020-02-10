# Spark Structured Streaming Evaluation

To compile: ``mvn package``

To run: ``spark-submit --class com.ncr.spark.client --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --master local[2] target/spark-client-1.0-SNAPSHOT-jar-with-dependencies.jar``

Required actions:

* Create input topic: ``kafka-topics --create --topic spark-streams-input --zookeeper localhost:2181 --partitions 1 --replication-factor 1``
* Create output topic: ``kafka-topics --create --topic spark-streams-output --zookeeper localhost:2181 --partitions 1 --replication-factor 1``
* Use Kafka producer: ``kafka-console-producer --broker-list localhost:9092 --topic spark-streams-input``
* Use Kafka producer with key: ``kafka-console-producer --broker-list localhost:9092 --topic spark-streams-input``
* Use Kafka producer with file: ``cat data_payload_one_record.json | kafka-console-producer --broker-list localhost:9092 --topic spark-streams-input``
* Use Kafka consumer: ``kafka-console-consumer --topic spark-streams-output --from-beginning --bootstrap-server localhost:9092``
