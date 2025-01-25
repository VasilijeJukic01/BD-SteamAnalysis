ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

ThisBuild / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED"
)

lazy val root = (project in file("."))
  .settings(
    name := "BD-SteamAnalysis"
  )

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.0",
  "org.apache.httpcomponents.client5" % "httpclient5" % "5.2",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2",
  "org.apache.parquet" % "parquet-avro" % "1.13.0"
)