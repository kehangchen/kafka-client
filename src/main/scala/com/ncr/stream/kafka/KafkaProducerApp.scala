package com.ncr.stream.kafka

import java.io.FileNotFoundException
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory

import scala.io.Source
object KafkaProducerApp extends App {
  lazy val logger = LoggerFactory.getLogger(getClass)

  val url = getClass.getResource("application.properties")
  val kafka_props: Properties = new Properties()

  if (url != null) {
    val source = Source.fromURL(url)
    kafka_props.load(source.bufferedReader())
  }
  else {
    logger.error("properties file cannot be loaded at path " + url)
    throw new FileNotFoundException("Properties file cannot be loaded")
  }

  val producer = new KafkaProducer[String, String](kafka_props)
  val topic = kafka_props.getProperty("topic_name")
  try {
    for (i <- 0 to 15) {
      val record = new ProducerRecord[String, String](topic, i.toString, "This is message we put into Kafka " + i)
      val metadata = producer.send(record)
      val message: String =  "sent record(key=" + record.key() + " value=" + record.value() +
        ") metadata(partition=" + metadata.get().partition() + ", offset=" + metadata.get().offset() + ")"
      logger.info(message)
    }
  }catch{
    case e:Exception => logger.error(e.getLocalizedMessage)
  }finally {
    producer.close()
  }
}