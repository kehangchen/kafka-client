package com.ncr.stream.spark

import com.microsoft.azure.sqldb.spark.config.Config
import net.liftweb.json._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
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

    spark.sparkContext.setLogLevel("ERROR")

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
//      .option("endingOffsets", """{"spark-streams-output":{"0":2}}""")
      .load()
//      .selectExpr("CAST(value AS STRING)").as[String].toDF()
//      .write.mode("overwrite").format("text").save("/Users/kehangchen/Documents/rcg/ncr/spark-sqlserver/batch")

    import org.apache.spark.sql.functions._
    val dataDf = payload.selectExpr("CAST(value AS STRING) as json")
      .select(from_json($"json", schema) as "data")
      .filter("data.entity_id != null")
      .filter("data.entity_key != null")
      .selectExpr("CAST(data.entry_id AS STRING)", "CAST(data.entity_id AS STRING)", "CAST(data.entity_key AS STRING)")
//      .write.mode("overwrite").format("text").save("/Users/kehangchen/Documents/rcg/ncr/spark-sqlserver/batch")
//    dataDf.printSchema()

    // Generate running word count
//    val events = payload.flatMap(textLine => {
//        val raw = JsonParser.parse(textLine)
//        val metadata = raw \\ "header"
//        val JObject(body) = (raw \\ "Body")
//        val JArray(a) = body(0).value
//        ((a map (_ merge metadata)) map (JsonAST.compactRender(_).replace("header", "metadata"))).toArray.mkString("\n").split("\\n")
//    })

//    val sqlContext = dataDf.sqlContext
//    val config = Config(Map(
//        "url"          -> "localhost:1433",
//        "databaseName" -> "testdb",
//        "dbTable"      -> "dbo.event",
//        "user"         -> "sa",
//        "password"     -> "Password@123"
//    ))

//    import org.apache.spark.sql.SaveMode
//    dataDf.write.mode(SaveMode.Append).saveAsTable("event")
//    dataDf.writeStream
//      .format("jdbc")
//      .start("jdbc:sqlserver://localhost:1433;user=sa;password=Password@123;databaseName=testdb;")

//    val query = dataDf.writeStream
//      .format("streaming-jdbc")
//      .option("checkpointLocation", "/Users/kehangchen/Documents/rcg/ncr/spark-sqlserver")
//      .outputMode(OutputMode.Append)
//      .option(JDBCOptions.JDBC_URL, "jdbc:sqlserver://localhost:1433;databaseName=testdb")
//      .option(JDBCOptions.JDBC_TABLE_NAME, "event")
//      .option(JDBCOptions.JDBC_DRIVER_CLASS, "com.microsoft.sqlserver.jdbc.SQLServerDriver")
//      .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, "1")
//      .option("user", "sa")
//      .option("password", "Password@123")
//      .trigger(Trigger.Continuous("1 second"))
//      .start()
//      .awaitTermination()

    val query = dataDf.writeStream
      .outputMode(OutputMode.Append)
      .format("console")
      .start()
    query.awaitTermination()
}
