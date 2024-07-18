package com.movieRating.utils

import org.apache.spark.sql.DataFrame

/**
 * Common Util class for writer
 *
 */
class Writer {

  /**
   * write into single 1 json file
   *
   * @param df   dataframe the we need to save
   * @param path path where we need to save
   */
  def saveToJson(df: DataFrame, path: String): Unit = {
    df.coalesce(1)
      .write
      .mode("overwrite")
      .json(path)
  }

  /**
   * write into single 1 csv file
   *
   * @param df   dataframe the we need to save
   * @param path path where we need to save
   */
  def saveToCsv(df: DataFrame, path: String): Unit = {
    df.coalesce(1)
      .write
      .mode("overwrite")
      .csv(path)
  }

}
