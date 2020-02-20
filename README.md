To compile: ``mvn package``

To run: ``spark-submit --class com.ncr.stream.spark.SparkStreamSQLServerApp --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0,org.apache.bahir:spark-sql-streaming-jdbc_2.11:2.4.0 --master local[2] target/spark-sqlserver-1.0-SNAPSHOT-jar-with-dependencies.jar``

To run metadata class:

* Create input topic: ``kafka-topics --create --topic spark-streams-input --zookeeper localhost:2181 --partitions 1 --replication-factor 1``
* Create output topic: ``kafka-topics --create --topic spark-streams-output --zookeeper localhost:2181 --partitions 1 --replication-factor 1``
* Use Kafka producer: ``kafka-console-producer --broker-list localhost:9092 --topic spark-streams-input``
* Use Kafka producer with key: ``kafka-console-producer --broker-list localhost:9092 --topic spark-streams-input --property "parse.key=true" --property "key.separator=:"``
* Use Kafka producer with file: ``cat data_payload_one_record.json | kafka-console-producer --broker-list localhost:9092 --topic spark-streams-input``
* Use Kafka consumer: ``kafka-console-consumer --topic spark-streams-output --from-beginning --bootstrap-server localhost:9092 --property print.key=true  --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer``


References:
* Processing Data in Apache Kafka with Structured Streaming in Apache Spark 2.2 - https://databricks.com/blog/2017/04/26/processing-data-in-apache-kafka-with-structured-streaming-in-apache-spark-2-2.html
* Interesting way to create Spark Json schema - https://stackoverflow.com/questions/48361177/spark-structured-streaming-kafka-convert-json-without-schema-infer-schema