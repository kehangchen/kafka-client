package com.ncr.stream.spark

import com.microsoft.azure.sqldb.spark.config.Config
import net.liftweb.json._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger

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

    import spark.implicits._

    val payload = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "spark-streams-output")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", """{"spark-streams-output":{"0":2}}""")
      .load()
      .selectExpr("CAST(value AS STRING)").as[String].toDF()
        .write.mode("overwrite").format("text").save("/Users/kehangchen/Documents/rcg/ncr/spark-sqlserver/batch")


    // Generate running word count
//    val events = payload.flatMap(textLine => {
//        val raw = JsonParser.parse(textLine)
//        val metadata = raw \\ "header"
//        val JObject(body) = (raw \\ "Body")
//        val JArray(a) = body(0).value
//        ((a map (_ merge metadata)) map (JsonAST.compactRender(_).replace("header", "metadata"))).toArray.mkString("\n").split("\\n")
//    })

//    val config = Config(Map(
//        "url"          -> "localhost:1433",
//        "databaseName" -> "testdb",
//        "dbTable"      -> "dbo.event",
//        "user"         -> "sa",
//        "password"     -> "Password@123"
//    ))
//
//    import org.apache.spark.sql.SaveMode
//    val collection = sqlContext.read.sqlDB(config)
//    payload.write.mode(SaveMode.Append).

}
