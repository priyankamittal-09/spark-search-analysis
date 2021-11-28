package com

import cats.data.Reader
import com.data.model.{CompetitorAppearances, RelevantCompetitors, RelevantSearchTerms, Results, ScrapeAppearances, Volumes}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoders, SparkSession}

package object data {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession
    .builder
    .appName("search-data-analysis")
    .master("local[*]")
    .getOrCreate()

  val conf: Config = ConfigFactory.load()
  val settings: Settings = new Settings(conf)

  /** Set aws access key & secret key via spark-submit */
  //spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", settings.awsAccessKey)
  //spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", settings.awsSecretKey)
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", settings.s3Endpoint)
  spark.sparkContext.hadoopConfiguration.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
  spark.sparkContext.hadoopConfiguration.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

  val competitorAppearancesSchema = Encoders.product[CompetitorAppearances].schema
  val relevantCompetitorsSchema = Encoders.product[RelevantCompetitors].schema
  val relevantSearchTermsSchema = Encoders.product[RelevantSearchTerms].schema
  val scrapeAppearancesSchema = Encoders.product[ScrapeAppearances].schema
  val volumesSchema = Encoders.product[Volumes].schema
  val resultSchema = Encoders.product[Results].schema

  type SparkSessionReader[A] = Reader[SparkSession, A]

  object SparkSessionReader {
    def apply[A](f: SparkSession => A): SparkSessionReader[A] =
      Reader[SparkSession, A](f)

    def lift[A](x: A): SparkSessionReader[A] =
      Reader[SparkSession, A](_ => x)
  }
}
