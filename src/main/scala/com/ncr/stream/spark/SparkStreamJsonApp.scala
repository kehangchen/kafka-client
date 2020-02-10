package com.ncr.stream.spark

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.streaming.Trigger
import net.liftweb.json._

object SparkStreamJsonApp extends App {

    var logger = Logger.getLogger(this.getClass())

    val jobName = "stream-kafka-topics"

    val conf = new SparkConf().setAppName(jobName)
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder
      .master("local")
      .appName("spark-stream-application")
      .getOrCreate()

    import spark.implicits._
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "spark-streams-input")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .flatMap(textLine => {
        val raw = JsonParser.parse(textLine)
        val metadata = raw \\ "header"
        val JObject(body) = (raw \\ "Body")
        val JArray(a) = body(0).value
        ((a map (_ merge metadata)) map (JsonAST.compactRender(_).replace("header", "metadata")))
          .toArray
          .mkString("\n")
          .split("\\n")
      })
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "spark-streams-output")
      .option("checkpointLocation", "/Users/kehangchen/Documents/rcg/ncr/spark-client")
      .trigger(Trigger.Continuous("1 second"))
      .start()
      .awaitTermination()
}
