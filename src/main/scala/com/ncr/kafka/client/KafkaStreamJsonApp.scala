package com.ncr.kafka.client

import java.time.Duration
import java.util.Properties

import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.slf4j.LoggerFactory

import scala.util.parsing.json._

object KafkaStreamJsonApp extends App {

  lazy val logger = LoggerFactory.getLogger(getClass)
  import org.apache.kafka.streams.scala.Serdes._
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import scala.collection.mutable.ListBuffer

  val config: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "in-metadata-application")
    val bootstrapServers = if (args.length > 0) args(0) else "localhost:9092"
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    p
  }

  val outConfig: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "out-metadata-application")
    val bootstrapServers = if (args.length > 0) args(0) else "localhost:9092"
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    p
  }

  val outBuilder = new StreamsBuilder()
  val outRecord: KStream[String, String] = outBuilder.stream[String, String]("streams-metadata-output")

  val builder = new StreamsBuilder()
  val payload: KStream[String, String] = builder.stream[String, String]("streams-metadata-input")
  val jsonArray: KStream[String, List[Any]] = payload
    .flatMapValues(textLine => {
      for {
        Some(M(map)) <- List(JSON.parseFull(textLine))
        M(metadata) = map("header")
        L(body) = map("Body")
      } yield {
        body.foreach(m => outRecord.mapValues(m => JSONObject(m.asInstanceOf[Map[String, Any]]).toString()).to("streams-metadata-output"))
        body
      }
    })

  val outStreams: KafkaStreams = new KafkaStreams(outBuilder.build(), outConfig)
  outStreams.cleanUp()
  outStreams.start()
  //jsonArray.to("streams-metadata-output")
  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
  streams.cleanUp()
  streams.start()

  // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
  sys.ShutdownHookThread {
    outStreams.close(Duration.ofSeconds(1))
    streams.close(Duration.ofSeconds(1))
  }

  class CC[T] { def unapply(a:Any):Option[T] = Some(a.asInstanceOf[T]) }

  object M extends CC[Map[String, Any]]
  object L extends CC[List[Any]]
  object I extends CC[List[String]]
  object S extends CC[String]
  object D extends CC[Double]
  object B extends CC[Boolean]
}