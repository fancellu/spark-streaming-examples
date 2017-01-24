name := "Spark-streaming-examples"

version := "1.0"

organization := "com.felstar"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0"
)