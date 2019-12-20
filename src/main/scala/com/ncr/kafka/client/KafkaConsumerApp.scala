package com.ncr.kafka.client

import java.io.{FileInputStream, FileNotFoundException}
import java.nio.file.Files
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

  val props:Properties = new Properties()
  props.put("group.id", "test")
  props.put("bootstrap.servers","localhost:9092")
  props.put("key.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer",
    "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  val consumer = new KafkaConsumer(props)
  val topics = List("text_topic")
  try {
    consumer.subscribe(topics.asJava)
    while (true) {
      val records = consumer.poll(10)
      for (record <- records.asScala) {
        println("Topic: " + record.topic() +
          ",Key: " + record.key() +
          ",Value: " + record.value() +
          ", Offset: " + record.offset() +
          ", Partition: " + record.partition())
      }
    }
  }catch{
    case e:Exception => e.printStackTrace()
  }finally {
    consumer.close()
  }
}

