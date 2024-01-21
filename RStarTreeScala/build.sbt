ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "RStarTreeScala"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.3.0-SNAP4" % Test

libraryDependencies += "io.circe" %% "circe-core" % "0.15.0-M1"
libraryDependencies += "io.circe" %% "circe-parser" % "0.15.0-M1"
libraryDependencies += "io.circe" %% "circe-generic" % "0.15.0-M1"

