package com.movieRating.utils

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ConfigTest extends AnyFunSuite with Matchers {
  val config = new LocalConfig("application-test.conf")

  test("getInputPath should return the correct path for ratings") {
    val ratingsConfig = config.getInputPath("ratings")
    ratingsConfig.path should be("src/main/resources/ratings.csv")
  }

  test("getOutputPath should return the correct path for json") {
    val jsonOutputConfig = config.getOutputPath("json")
    jsonOutputConfig.path should be("src/main/resources/result_json")
  }

  test("getValue should throw exception for missing config key") {
    intercept[IllegalArgumentException] {
      config.getInputPath("non_existent_key")
    }
  }

  test("getValue should throw exception for wrong type config key") {
    intercept[IllegalArgumentException] {
      config.getOutputPath("wrong_type_key")
    }
  }
}