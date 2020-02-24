package com.ncr.config

import java.io.File
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}

class NCRConfig(fileNameOption: Option[String] = None) {

  val config = if (fileNameOption.getOrElse("").isEmpty()) ConfigFactory.load()
    else ConfigFactory.systemProperties().withFallback(ConfigFactory.systemEnvironment().withFallback(ConfigFactory.parseFile(new File(fileNameOption.getOrElse("")))).resolve())

  def envOrElseConfig(name: String): String = {
    scala.util.Properties.envOrElse(
      name.toUpperCase.replaceAll("""\.""", "_"),
      config.getString(name)
    )
  }

  def getConfig(path: String): Config = {
    config.getConfig(path)
  }

  def getProperties(path: String): Properties = {
    import scala.collection.JavaConversions._

    val props = new Properties()

    val map: Map[String, Object] = config.getConfig(path).entrySet().map({ entry =>
      entry.getKey -> entry.getValue.unwrapped()
    })(collection.breakOut)

    props.putAll(map)
    props
  }
}