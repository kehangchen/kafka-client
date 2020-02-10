package org.example

import java.time.Duration
import java.util.Date

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.joda.time.DateTime

import scala.collection.JavaConverters._
object KafkaConsumerApp extends App {


  val path = "/home/neena/NCR/KafkaStreamTest/ncr.conf"

  val ncrConfig = new NCRConfig(Option(path))
  val kafka_props = ncrConfig.getProperties("ncr-config.kafka.consumer")

  val consumer = new KafkaConsumer(kafka_props)
  val topic_string = kafka_props.getProperty("topic_name")
  val topics =  topic_string.split(',').toList
  var recordCount = 0
  var totalTime = 0L
  var recordNumber = 1L

  try {
    consumer.subscribe(topics.asJava)
    println("-----------Consuming from "+topic_string+"....................")
    while (true) {
      val records = consumer.poll(Duration.ofSeconds(10))
      var firstTime = 0L
      recordCount += records.count()
      for (record <- records.asScala) {
        var timeDiff = 0L
        if (firstTime != 0L){
          timeDiff = record.timestamp() - firstTime
          firstTime = record.timestamp()
          totalTime  += timeDiff
        }
        else{
          firstTime = record.timestamp()
        }
        println("Record : "+recordNumber +"  "+"Timestamp : "+record.timestamp()+" "+"Duration : "+timeDiff)
        recordNumber += 1
      }
      println("total time : "+totalTime+ " "+"recordCount : "+recordCount)

    }
  }catch{
    case e:Exception => //logger.error(e.getLocalizedMessage)
  }finally {
    consumer.close()
  }
}

