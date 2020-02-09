package com.ncr.stream.exception

import java.util
import java.util.Properties

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.streams.errors.ProductionExceptionHandler
import org.apache.kafka.streams.errors.ProductionExceptionHandler.ProductionExceptionHandlerResponse
/*
By default, Kafka provides and uses the DefaultProductionExceptionHandler that always fails
when these exceptions occur.

This exception handler will ignore record is too large exception and continue to process records.
 */
class IgnoreRecordTooLargeHandler extends ProductionExceptionHandler {

  def handle(record: ProducerRecord[Array[Byte], Array[Byte]], exception: Exception): ProductionExceptionHandlerResponse = {
    if (exception.isInstanceOf[RecordTooLargeException]) {
      // TODO - need to allow plugin to log the too large record into a storage, such as ADLS Gen2 storage
      ProductionExceptionHandlerResponse.CONTINUE;
    } else {
      ProductionExceptionHandlerResponse.FAIL;
    }
  }

  override def configure(map: util.Map[String, _]): Unit = ???
}

object IgnoreRecordTooLargeHandler {
  val settings = new Properties();

  // other various kafka streams settings, e.g. bootstrap servers, application id, etc
  settings.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, IgnoreRecordTooLargeHandler)
}