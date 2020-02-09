package com.ncr.stream.kafka

import java.io.{FileInputStream, FileNotFoundException}
import java.nio.file.Files
import java.time.Duration
import java.util.{Collections, Properties}
import java.util.regex.Pattern

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
object KafkaConsumerApp extends App {

  lazy val logger = LoggerFactory.getLogger(getClass)

  val path = "/Volumes/Macintosh HD2/Users/kehangchen/Documents/mes/scala/kafka-client/consumer.properties"
  val in = new FileInputStream(path)
  val kafka_props: Properties = new Properties()
  if (in != null) {
    try {
      kafka_props.load(in)
    }
    finally {
      in.close()
    }
  }
  else {
    logger.error("properties file cannot be loaded at path " + path)
    throw new FileNotFoundException("Properties file cannot be loaded")
  }

  val consumer = new KafkaConsumer(kafka_props)
  val topic_string = kafka_props.getProperty("topic_name")
  val topics =  topic_string.split(',').toList
  try {
    consumer.subscribe(topics.asJava)
    while (true) {
      val records = consumer.poll(Duration.ofSeconds(10))
      for (record <- records.asScala) {
        logger.info("Topic: " + record.topic() +
          ",Key: " + record.key() +
          ",Value: " + record.value() +
          ", Offset: " + record.offset() +
          ", Partition: " + record.partition())
      }
    }
  }catch{
    case e:Exception => logger.error(e.getLocalizedMessage)
  }finally {
    consumer.close()
  }
}

