package com.ncr.stream.spark

import java.sql.{Connection, DriverManager}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object SparkStreamSQLServerApp extends App {

  var logger = Logger.getLogger(this.getClass())

  val jobName = "spark-sqlserver-topics"

  val conf = new SparkConf().setAppName(jobName)
  val sc = new SparkContext(conf)

  val spark = SparkSession.builder
    .master("local")
    .appName("spark-sqlserver-application")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val schema = spark
    .read
    .json("/Users/kehangchen/Documents/rcg/ncr/spark-sqlserver/schema.txt")
    .schema

  import spark.implicits._

  val payload = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "spark-streams-output")
    .option("startingOffsets", "earliest")
    .load()

  import org.apache.spark.sql.functions._

  val dataDf = payload.selectExpr("CAST(value AS STRING) as json")
    .select(from_json($"json", schema) as "data")
    .selectExpr("CAST(data.entry_id AS INTEGER)", "CAST(data.entity_id AS STRING)", "CAST(data.entity_key AS STRING)")
    .where("entity_id != ''")

  dataDf.writeStream.trigger(Trigger.ProcessingTime("60 seconds"))
    .outputMode(OutputMode.Update())
    .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
      insertToTable(batchDf,
        "jdbc:sqlserver://localhost:1433;databaseName=testdb;user=sa;password=Password@123",
        "event")
    }
    .start()
    .awaitTermination()

   /**
   * Insert in to database using foreach partition.
   *
   * @param dataframe : DataFrame
   * @param sqlDatabaseConnectionString
   * @param sqlTableName
   */
  def insertToTable(dataframe: DataFrame, sqlDatabaseConnectionString: String, sqlTableName: String): Unit = {
    val SQL = """MERGE
      INTO %s WITH (HOLDLOCK) AS target
      USING (SELECT %s) AS source
        (%s)
      ON (target.entry_id = source.entry_id)
      WHEN NOT MATCHED
        THEN INSERT (%s)
        VALUES %s;"""
    //numPartitions = number of simultaneous DB connections you can planning to give
    dataframe.repartition(20)

    val tableHeader: String = dataframe.columns.mkString(",")
    val headerList: Array[String] = dataframe.columns
    dataframe.foreachPartition { partition =>
      // Note : Each partition one connection (more better way is to use connection pools)
      val sqlExecutorConnection: Connection = DriverManager.getConnection(sqlDatabaseConnectionString)
      //Batch size of 1000 is used since some databases cant use batch size more than 1000 for ex : Azure sql
      partition.grouped(1000).foreach {
        group =>
          group.foreach {
            record =>
              val insertString: scala.collection.mutable.StringBuilder = new scala.collection.mutable.StringBuilder()
              insertString.append("('" + record.mkString("','") + "')")
              val valuesStringTemplate: scala.collection.mutable.StringBuilder = new scala.collection.mutable.StringBuilder()
              valuesStringTemplate.append("'" + record.mkString("' AS %s,'") + "' AS %s")

              val valuesString = valuesStringTemplate.format(headerList: _*)
              logger.error("SQL: " + SQL)
              val sql = SQL.format(sqlTableName, valuesString, tableHeader, tableHeader, insertString)
              logger.error(sql)
              sqlExecutorConnection.createStatement().executeUpdate(sql)
          }
      }
      sqlExecutorConnection.close() // close the connection
    }
  }
}
