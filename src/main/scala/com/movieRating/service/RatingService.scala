package com.movieRating.service

import org.apache.spark.sql.functions.{array_contains, col}
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
 * Service class that implement logic of rating aggregation
 *
 * @param spark §Spark session
 */
class RatingService(spark: SparkSession) {

  import spark.implicits._

  val RATING = "rating"
  val MOVIE = "movie"
  val MOVIE_ID = "movieId"
  val COUNT = "count"
  val TITLE = "title"

  /**
   * The method is designed to create the number of grades given in the following order:
   * “1”, ‘2’, ‘3’, ‘4’, ‘5’. That is, how many were ones, twos, threes, etc.
   * In the “hist_all” field you should specify the same thing only for all movies the total number
   * of grades given in the same order: “1”, ‘2’, ‘3’, ‘4’, ‘5’.
   *
   * @param ratingsDF DataFrame of the rating table
   * @param moviesDF  DataFrame of the movies table
   * @param linksDF   DataFrame of the links table
   * @param movieId   Id of the movie that we looking for
   * @return DataFrame with such schema ("movie", "hist_movie", "hist_all")
   *         where  movie - is the Sting name if the movie
   *         hist_movie - array of the rating count. how many 1 star, 2 starts ... til five
   *         hist_all - count of the starts across of the whole data
   */
  def calculateRatingStatisticsForMovie(ratingsDF: DataFrame, moviesDF: DataFrame, linksDF: DataFrame, movieId: Int): DataFrame = {
    val movieRatings = ratingsDF.filter(col(MOVIE_ID) === movieId)
      .groupBy(RATING)
      .count()
      .orderBy(RATING)

    val allRatings = ratingsDF.groupBy(RATING)
      .count()
      .orderBy(RATING)

    val movieRatingsList =
      (1 to 5)
        .map(r => movieRatings.filter(col(RATING) === r)
          .select(COUNT)
          .as[Long]
          .collect()
          .headOption
          .getOrElse(0L))
        .toList

    val allRatingsList = (1 to 5)
      .map(r => allRatings.filter(col(RATING) === r)
        .select(COUNT)
        .as[Long].collect().headOption.getOrElse(0L))
      .toList

    val movieTitle = moviesDF
      .filter(col(MOVIE_ID) === movieId)
      .select(TITLE).as[String]
      .collect()
      .headOption
      .getOrElse("Unknown Movie")


    Seq(
      (movieTitle, movieRatingsList, allRatingsList)
    ).toDF(MOVIE, "hist_movie", "hist_all")
  }

  /**
   * Method is created to create a list of all movies of a given genre, along with its link imdbId, tmdbId(links.csv file).
   *
   * @param moviesDF DataFrame of the movies table
   * @param linksDF  DataFrame of the links table
   * @param genre    type of the movie that we looking for
   * @return
   */
  def generateMovieLinksCSV(moviesDF: DataFrame, linksDF: DataFrame, genre: String): DataFrame = {

    val genreMovies = moviesDF.filter(array_contains(org.apache.spark.sql.functions.split($"genres", "\\|"), genre))

    val movieLinks = genreMovies
      .join(linksDF, Seq(MOVIE_ID), "left")
      .select(TITLE, "imdbId", "tmdbId")
      .orderBy(TITLE)

    movieLinks
  }

}
