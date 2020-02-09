To compile: ``mvn package``

To run: ``spark-submit --class com.ncr.spark.client --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --master local[2] target/spark-client-1.0-SNAPSHOT-jar-with-dependencies.jar``
