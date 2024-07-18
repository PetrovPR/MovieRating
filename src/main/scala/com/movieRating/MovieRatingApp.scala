package com.movieRating

import com.movieRating.service.RatingService
import com.movieRating.utils.{LocalConfig, Reader, Writer}
import org.apache.spark.sql.SparkSession

/**
 * Main Application class for running current application
 */
object MovieRatingApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MovieLensProject")
      .master("local[*]")
      .getOrCreate()

    val dataReader = new Reader(spark)
    val writer = new Writer()
    val ratingService = new RatingService(spark)

    val config = new LocalConfig("application.conf")

    val ratingsConfig = config.getInputPath("ratings")
    val moviesConfig = config.getInputPath("movies")
    val linksConfig = config.getInputPath("links")

    val ratingsDF = dataReader.readCsv(ratingsConfig)
    val moviesDF = dataReader.readCsv(moviesConfig).cache()
    val linksDF = dataReader.readCsv(linksConfig).cache()

    val movieRatingDF = ratingService.calculateRatingStatisticsForMovie(ratingsDF, moviesDF, linksDF, 1)
    val movieGenreDF = ratingService.generateMovieLinksCSV(moviesDF, linksDF, "Children")

    writer.saveToCsv(movieGenreDF, config.getOutputPath("csv").path)
    writer.saveToJson(movieRatingDF, config.getOutputPath("json").path)

    spark.stop()
  }

}
