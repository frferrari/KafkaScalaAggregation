name := "KafkaStreamsSessionWindowScala"

version := "0.1"

resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.4.0",
  "org.apache.kafka" % "kafka-streams" % "2.5.0",
  "org.apache.kafka" % "kafka-clients" % "2.5.0",
  "org.apache.kafka" % "kafka-streams-scala_2.12" % "2.5.0",
  "org.slf4j" % "slf4j-log4j12" % "1.7.30",
  "io.argonaut" %% "argonaut" % "6.2.2",
  "org.scalactic" %% "scalactic" % "3.0.0",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test"
)

scalaVersion := "2.12.8"

logBuffered in Test := false
