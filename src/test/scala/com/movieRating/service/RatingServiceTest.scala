package com.movieRating.service

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RatingServiceTest extends  AnyFunSuite with DataFrameSuiteBase with Matchers {

  test("calculateRatingStatisticsForMovie should return correct DataFrame") {

    import spark.implicits._

    val ratingService = new RatingService(spark)
    val ratingsDF = Seq(
      (1, 1, 5.0, 964982703),
      (2, 2, 3.0, 964982224),
      (3, 1, 4.0, 964983815),
      (4, 1, 2.0, 964982933),
      (5, 1, 1.0, 964982933),
      (6, 2, 4.0, 964982224)
    ).toDF("userId", "movieId", "rating", "timestamp")

    val moviesDF = Seq(
      (1, "Toy Story", "Children"),
      (2, "Jumanji", "Adventure")
    ).toDF("movieId", "title", "genres")

    val linksDF = Seq(
      (1, "0114709", "862"),
      (2, "0113497", "8844")
    ).toDF("movieId", "imdbId", "tmdbId")

    val resultDF = ratingService.calculateRatingStatisticsForMovie(ratingsDF, moviesDF, linksDF, 1)

    val expectedHistMovie = Seq(1L, 1L, 0L, 1L, 1L)
    val expectedHistAll = Seq(1L, 1L, 1L, 2L, 1L)

    resultDF.select("movie").as[String].collect() should contain("Toy Story")
    resultDF.select("hist_movie").as[Seq[Long]].collect().head should be(expectedHistMovie)
    resultDF.select("hist_all").as[Seq[Long]].collect().head should be(expectedHistAll)
  }

  test("generateMovieLinksCSV should return correct DataFrame") {

    import spark.implicits._

    val ratingService = new RatingService(spark)
    val moviesDF = Seq(
      (1, "Toy Story", "Children"),
      (2, "Jumanji", "Adventure"),
      (3, "Lion King", "Children")
    ).toDF("movieId", "title", "genres")

    val linksDF = Seq(
      (1, "0114709", "862"),
      (2, "0113497", "8844"),
      (3, "0110357", "158")
    ).toDF("movieId", "imdbId", "tmdbId")

    val resultDF = ratingService.generateMovieLinksCSV(moviesDF, linksDF, "Children")
    resultDF.columns should contain allOf ("title", "imdbId", "tmdbId")
    resultDF.count() should be(2)
  }

  test("calculateRatingStatisticsForMovie should handle movies with no ratings") {
    import spark.implicits._

    val ratingService = new RatingService(spark)
    val ratingsDF = Seq(
      (1, 2, 5.0, 964982703),
      (2, 2, 3.0, 964982224)
    ).toDF("userId", "movieId", "rating", "timestamp")

    val moviesDF = Seq(
      (1, "Toy Story", "Children"),
      (2, "Jumanji", "Adventure")
    ).toDF("movieId", "title", "genres")

    val linksDF = Seq(
      (1, "0114709", "862"),
      (2, "0113497", "8844")
    ).toDF("movieId", "imdbId", "tmdbId")

    val resultDF = ratingService.calculateRatingStatisticsForMovie(ratingsDF, moviesDF, linksDF, 1)

    val expectedHistMovie = Seq(0L, 0L, 0L, 0L, 0L)
    val expectedHistAll = Seq(0L, 0L, 1L, 0L, 1L)

    resultDF.select("movie").as[String].collect() should contain("Toy Story")
    resultDF.select("hist_movie").as[Seq[Long]].collect().head should be(expectedHistMovie)
    resultDF.select("hist_all").as[Seq[Long]].collect().head should be(expectedHistAll)

  }

  test("calculateRatingStatisticsForMovie should handle missing movieId") {
    import spark.implicits._

    val ratingService = new RatingService(spark)
    val ratingsDF = Seq(
      (1, 1, 5.0, 964982703),
      (2, 2, 3.0, 964982224),
      (3, 1, 4.0, 964983815),
      (4, 1, 2.0, 964982933),
      (5, 1, 1.0, 964982933),
      (6, 2, 4.0, 964982224),
      (6, 1, 4.0, 964982223)
    ).toDF("userId", "movieId", "rating", "timestamp")

    val moviesDF = Seq(
      (2, "Jumanji", "Adventure")
    ).toDF("movieId", "title", "genres")

    val linksDF = Seq(
      (1, "0114709", "862"),
      (2, "0113497", "8844")
    ).toDF("movieId", "imdbId", "tmdbId")

    val resultDF = ratingService.calculateRatingStatisticsForMovie(ratingsDF, moviesDF, linksDF, 1)

    val expectedHistMovie = Seq(1L, 1L, 0L, 2L, 1L)
    val expectedHistAll = Seq(1L, 1L, 1L, 3L, 1L)

    resultDF.select("movie").as[String].collect() should contain("Unknown Movie")
    resultDF.select("hist_movie").as[Seq[Long]].collect().head should be(expectedHistMovie)
    resultDF.select("hist_all").as[Seq[Long]].collect().head should be(expectedHistAll)
  }
}
