name := "KafkaStreamsSessionWindowScala"

version := "0.1"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.4.0",
  "org.apache.kafka" % "kafka-streams" % "2.1.0",
  "org.apache.kafka" % "kafka-clients" % "2.1.0",
  "org.apache.kafka" % "kafka-streams-scala_2.12" % "2.1.0",
  "org.slf4j" % "slf4j-log4j12" % "1.7.30",
  // "com.fasterxml.jackson.core" % "jackson-databind" % "2.11.0",
  // "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.11.0",
  "io.argonaut" %% "argonaut" % "6.2.2"
)

scalaVersion := "2.12.8"
