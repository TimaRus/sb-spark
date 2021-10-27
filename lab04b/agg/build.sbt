name := "agg"
version := "1.0"
scalaVersion := "2.11.12"

val sparkVersion = "2.4.7"
val kafkaVersion = "2.4.5"

libraryDependencies ++= Seq(
  // spark core
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % kafkaVersion
)