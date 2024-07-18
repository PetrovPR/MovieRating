package com.movieRating.utils

import com.typesafe.config.{Config, ConfigException, ConfigFactory}

case class DataConfig(path: String)

/**
 * configuration reader class
 *
 * @param configPath path of the configuration
 */
class LocalConfig(configPath: String) {
  private val config: Config = ConfigFactory.load(configPath)

  /**
   * get value from configuration file
   *
   * @param path path inside of the config file
   * @param name attribute name that we need to find
   * @return value of the configuration ot rise up an exception
   */
  private def getValue(path: String, name: String): String = {
    val fullKey = s"$path.$name"
    try {
      config.getString(fullKey)
    } catch {
      case _: ConfigException.Missing =>
        throw new IllegalArgumentException(s"Configuration error: Missing $fullKey in application.conf")
      case _: ConfigException.WrongType =>
        throw new IllegalArgumentException(s"Configuration error: Wrong type for $fullKey in application.conf")
      case ex: Throwable =>
        throw new IllegalArgumentException(s"Configuration error: Error reading $fullKey from application.conf", ex)
    }
  }

  /**
   * get input value from configuration file
   *
   * @param name attribute of witch path we need to get
   * @return DataConfig
   */
  def getInputPath(name: String): DataConfig = {
    val path = getValue(s"data.input_file_path.$name", "path")
    DataConfig(path)
  }

  /**
   * get output value from configuration file
   *
   * @param name attribute of witch path we need to get
   * @return DataConfig
   */
  def getOutputPath(name: String): DataConfig = {
    val path = getValue("data.output_path", name)
    DataConfig(path)
  }

}
