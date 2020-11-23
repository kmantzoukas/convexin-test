name := "convexin-test"

version := "0.1"

scalaVersion := "2.12.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.hadoop" % "hadoop-aws" % "2.6.0",
  "com.amazonaws" % "aws-java-sdk" % "1.11.698"
)
