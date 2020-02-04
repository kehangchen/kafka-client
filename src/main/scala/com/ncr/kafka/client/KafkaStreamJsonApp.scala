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

  val config: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "in-metadata-application")
    val bootstrapServers = if (args.length > 0) args(0) else "localhost:9092"
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    p
  }

  val builder = new StreamsBuilder()
  val payload: KStream[String, String] = builder.stream[String, String]("source_topic")
  val jsonArray: KStream[String, String] = payload
    .flatMapValues(textLine => {
      for {
        Some(M(map)) <- List(JSON.parseFull(textLine))
        M(metadata) = map("header")
        S(body) = (map("Body").asInstanceOf[List[Any]] map (_.toString)).toArray.mkString("\n")
      } yield {
        val stringBody : Array[String] = body.split("\n")
        val stringBodyBuffer : StringBuffer = new StringBuffer()
        for(i <- 0 until stringBody.length){
          stringBodyBuffer.append("{"+stringBody(i).toString.concat(",metadata:").concat(new JSONObject(metadata).toString())+"}")
        }
        stringBodyBuffer.toString
      }
    })

  jsonArray.to("hub_topic")
  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
  streams.cleanUp()
  streams.start()

  // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(1))
  }

  class CC[T] { def unapply(a:Any):Option[T] = Some(a.asInstanceOf[T]) }

  object M extends CC[Map[String, Any]]
  object L extends CC[Array[String]]
  object I extends CC[List[String]]
  object S extends CC[String]
  object D extends CC[Double]
  object B extends CC[Boolean]
}