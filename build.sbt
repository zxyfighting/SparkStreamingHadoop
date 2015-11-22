resolvers += "Nexus" at "http://nexus:8081/nexus/content/groups/public"

val spark_core_version = "1.5.2"
val scala_version      = "2.10.4"

val spark                 = "org.apache.spark"  % "spark-streaming-kafka_2.10" % spark_core_version
val spark_core            = "org.apache.spark"  % "spark-core_2.10"            % spark_core_version
val spark_streaming       = "org.apache.spark"  % "spark-streaming_2.10"       % spark_core_version

mainClass in (Compile,run) := Some("ReceiverBased")

lazy val commonSettings = Seq(
  organization := "com.example",
  version := "1.0",
  scalaVersion := scala_version
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "receiver-based",
    libraryDependencies += spark,
    libraryDependencies += spark_core,
    libraryDependencies += spark_streaming
  )
