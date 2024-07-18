package com.movieRating.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Class for reading files
 *
 * @param spark current SparkSession
 */
class Reader(spark: SparkSession) {

  /**
   * implement logic of reading files
   *
   * @param dataConfig data configuration
   * @return data frame file that we read
   */
  def readCsv(dataConfig: DataConfig): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(dataConfig.path)
  }

}
