package com.ncr.kafka.client

import java.time.Duration
import java.util.Properties

import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.slf4j.LoggerFactory

import scala.util.parsing.json._
//import com.ncr.kafka.json.JSON._
//import net.minidev.json



object KafkaStreamJsonApp extends App {

  lazy val logger = LoggerFactory.getLogger(getClass)
  import org.apache.kafka.streams.scala.Serdes._
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import net.liftweb.json._

//  JSON.globalNumberParser = {in => try in.toLong catch { case _: NumberFormatException => in.toDouble}}

  val config: Properties = {
    val p = new Properties()
    // this parameter must be unique within a Kafka cluster
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "in-metadata-application")
    val bootstrapServers = if (args.length > 0) args(0) else "localhost:9092"
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    p
  }

  val builder = new StreamsBuilder()
  val payload: KStream[String, String] = builder.stream[String, String]("streams-metadata-input")
  val jsonArray: KStream[String, String] = payload
    .flatMapValues(textLine => {
//      for {
//        Some(M(map)) <- List(JSON.parseFull(textLine))
//        M(metadata) = map("header")
//        S(body) = (map("Body").asInstanceOf[List[Map[String, Any]]] map (new JSONObject(_).toString() + (new JSONObject(metadata).toString()))).toArray.mkString("\n")
//      } yield {
//        body
//      }

//      val tree = parseJSON(textLine)
//      val metadata = tree.header
//      val body = tree.Body
//      metadata.toString.split("\\n")

      val raw = JsonParser.parse(textLine)
      val metadata = raw \\ "header"
      val JObject(body) = (raw \\ "Body")
      val JArray(a) = body(0).value
      val b = ((a map (_ merge metadata)) map (JsonAST.compactRender(_))).toArray.mkString("\n")
      b.split("\\n")
    })
//    .flatMapValues(json => json.split("\\n"))

  jsonArray.to("streams-metadata-output")
  val topology = builder.build(config)
  System.out.println(topology.describe())
  val streams: KafkaStreams = new KafkaStreams(topology, config)
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