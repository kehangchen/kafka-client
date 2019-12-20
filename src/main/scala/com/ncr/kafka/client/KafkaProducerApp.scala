package com.ncr.kafka.client

import java.io.FileNotFoundException
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.io.Source
object KafkaProducerApp extends App {

  val url = getClass.getResource("application.properties")
  val ext_props: Properties = new Properties()

  if (url != null) {
    val source = Source.fromURL(url)
    ext_props.load(source.bufferedReader())
  }
  else {
//    logger.error("properties file cannot be loaded at path " +path)
    throw new FileNotFoundException("Properties file cannot be loaded")
  }

//  val bootstrap_server = ext_props.getProperty("bootstrap.servers")
//
//  val kafka_props:Properties = new Properties()
//  kafka_props.put("bootstrap.servers",bootstrap_server)
//  kafka_props.put("key.serializer",ext_props.getProperty("key.serializer"))
//  kafka_props.put("value.serializer",ext_props.getProperty("value.serializer"))
//  kafka_props.put("acks",ext_props.getProperty("acks"))
//  val producer = new KafkaProducer[String, String](kafka_props)
  val producer = new KafkaProducer[String, String](ext_props)
  val topic = ext_props.getProperty("topic_name")
  try {
    for (i <- 0 to 15) {
      val record = new ProducerRecord[String, String](topic, i.toString, "My Site is sparkbyexamples.com " + i)
      val metadata = producer.send(record)
      printf(s"sent record(key=%s value=%s) " +
        "meta(partition=%d, offset=%d)\n",
        record.key(), record.value(),
        metadata.get().partition(),
        metadata.get().offset())
    }
  }catch{
    case e:Exception => e.printStackTrace()
  }finally {
    producer.close()
  }
}