name := "spark-search-analysis"

version := "0.1"

scalaVersion := "2.12.15"

val sparkVer = "3.2.0"
val specs2Version = "4.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVer,
  "org.apache.spark" %% "spark-sql" % sparkVer,
  "org.typelevel" %% "cats" % "0.9.0",
  // https://mvnrepository.com/artifact/com.typesafe/config
  "com.typesafe" % "config" % "1.4.1",
  // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
  "org.apache.hadoop" % "hadoop-common" % "3.3.1",
  // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client
  "org.apache.hadoop" % "hadoop-client" % "3.3.1",
  // https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws
  "org.apache.hadoop" % "hadoop-aws" % "3.3.1",
  // https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk
  "com.amazonaws" % "aws-java-sdk" % "1.12.115"
)

libraryDependencies ++= Seq (
  "org.specs2"                 %% "specs2-core"                    % specs2Version  ,
  "org.specs2"                 %% "specs2-mock"                    % specs2Version  ,
  "org.specs2"                 %% "specs2-matcher-extra"           % specs2Version
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
