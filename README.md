# Upserting Data from Kafka to SQL Database with Spark Structured Streaming Prototype 

This prototype will need to work with kafka-client or spark-client application that reads "event" payload and explodes it into many events.  It extracts three fields from each exploded event and then upserts them into a SQL Server database table with "entry_id" as the unique key.

## Prerequisite

If you can access an Azure SQL Database instance and create table, you should not need to use SQL Server docker for this test.  Following is the list of required actions,

1. Start a SQL Server docker instance: ``docker run -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=Password@123' -p 1433:1433 -d mcr.microsoft.com/mssql/server:2017-latest``
2. Copy the "CONTAINER ID" for the SQL Server docker instance from the output of: ``docker ps``
3. Access the SQL Server with "sqlcmd": ``docker exec -it <CONTAINER ID> /opt/mssql-tools/bin/sqlcmd -d testdb -S localhost -U sa -P Password@123``
4. Create the event table: ``create table event (entry_id int, entity_id varchar(255), entity_key varchar(255))``
5. Produce individual events into "spark-streams-output" topic using "kafka-client" or "spark-client" application we have done before.

## Execution

* Compile the code: ``mvn clean package``
* To run: ``spark-submit --class com.ncr.stream.spark.SparkStreamSQLServerApp --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 --master local[2] target/spark-sqlserver-1.0-SNAPSHOT-jar-with-dependencies.jar``
* Check result with "sqlcmd" from step 3 of Prerequisite and there should not have any record with duplicated "entry_id": ``select * from event``.

## Known Issues

* All environment parameters should be externalized
* In the current code, a sql statement with "Merge" and "When Not Merged" is used to deduplicate records with the same "entry_id".  It can be easily modfied to call stored procedure instead.  Please take a look at the "SQL" variable in "insertToTable" function and you can use it as the base for creating the stored procedure.  This sql statement is also missing "When Matched" action.  In real scenarios, "When Matched" action should be set the new values for the matched record
* "time" field should also be added as one of the condition for match, such as the time of the record must be later than the time of the existing record so it can handle late arrival records
* Also, this prototype code does not incorparate database connection pool.  However, it should be easily accomplished by using "SQLServerXADataSource" class or "c3p0" library
* If Azure SQL Database credentials are provided, you should be able to change the connection string in line 55 of "SparkStreamSQLServerApp.scala" file

