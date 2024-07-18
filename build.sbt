name := "MovieRating"

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.14"

resolvers ++= Seq(
  "Maven Central" at "https://repo1.maven.org/maven2",
  "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.0",
  "org.apache.spark" %% "spark-sql" % "3.3.0",
  "com.typesafe" % "config" % "1.4.1",
  "org.scalatest" %% "scalatest" % "3.2.12" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "3.3.1_1.3.0" % Test
)

javaOptions += "-Djava.base/sun.nio.ch=ALL-UNNAMED"

assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

Compile / mainClass := Some("com.movieRating.MovieRatingApp")