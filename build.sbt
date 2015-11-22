val derby = "org.apache.derby" % "derby" % "10.4.1.3"
val spark = "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.5.2"

mainClass in (Compile,run) := Some("ReceiverBased")

lazy val commonSettings = Seq(
  organization := "com.example",
  version := "1.0",
  scalaVersion := "2.9.2"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "receiver-based",
    libraryDependencies += derby,
    libraryDependencies += spark
  )
