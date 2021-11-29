package com.data

import com.data.utils.ReadWriteUtils
import com.data.model.{CompetitorAppearances, RelevantCompetitors, RelevantSearchTerms, Results, ScrapeAppearances, Volumes}
import com.data.utils.DateUtils.generateDatesInRange
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

object Main extends App{

  implicit val logger: Logger = Logger.getLogger(this.getClass)
  System.setProperty("com.amazonaws.sdk.disableCertChecking", settings.awsCertificateCheck)
  val date_range =  generateDatesInRange(settings.startDate, settings.endDate).toList.mkString("{", ",", "}")

  (for {
    scrape_appearances <- readScrapeAppearancesData(settings.scrapeAppearancesDataPath)
    competitor_appearances <- readCompetitorAppearancesData(settings.competitorAppearancesDataPath)
    volumes <- readVolumesData(settings.volumesDataPath)
    relevant_competitors <- readRelevantCompetitorsData(settings.relevantCompetitorsDataPath)
    relevant_search_terms <- readRelevantSearchTermsData(settings.relevantSearchTermsDataPath)
    frequencyStatsDF <- calculateFrequency(competitor_appearances, scrape_appearances)
    impressionsStatsDF <- calculateImpressions(frequencyStatsDF, volumes)
    result <- calculateResult(relevant_competitors, relevant_search_terms, impressionsStatsDF)
    _ <- writeResults(result, settings.resultPath)
    _ <- closeSparkSession()
  } yield ()).run(spark)


  import spark.implicits._

  /** TODO Read Scrape Appearances
   *
   * @param path s3 uri for Scrape Appearances
   * @return ScrapeAppearances dataset which gives loggerHow many times we scraped a given search term on a given day and device.
   */
  def readScrapeAppearancesData(path: String)(implicit logger:Logger): SparkSessionReader[Dataset[ScrapeAppearances]] = {
    SparkSessionReader { spark =>
      val s3URI = s"$path$date_range/*"
      val scrape_appearances = Try(ReadWriteUtils
        .readTSV(spark, scrapeAppearancesSchema, s3URI).get) match {
        case Success(df) =>
          df.as[ScrapeAppearances]
        case Failure(_) =>
          logger.error(s"Failed to read S3 data")
          spark.emptyDataset[ScrapeAppearances]
      }
      scrape_appearances
    }
  }


  /** TODO Read Competitor Appearances
   *
   * @param path s3 uri for Competitor Appearances
   * @return CompetitorAppearances dataset which gives How many times we saw a given domain appear on a Google search for a given search term on a given day and device.
   */
  def readCompetitorAppearancesData(path: String)(implicit logger: Logger): SparkSessionReader[Dataset[CompetitorAppearances]] = {
    SparkSessionReader { spark =>
      val s3URI = s"$path$date_range/*"
      val competitor_appearances = Try(ReadWriteUtils
        .readTSV(spark, competitorAppearancesSchema, s3URI).get) match {
        case Success(df) =>
          df.as[CompetitorAppearances]
        case Failure(_) =>
          logger.error(s"Failed to read S3 data")
          spark.emptyDataset[CompetitorAppearances]
      }
      competitor_appearances
    }
  }

  /** TODO Read Volumes
   *
   * @param path s3 uri for Volumes
   * @return Volumes dataset which gives How many times do people normally search for a given term per month.
   */
  def readVolumesData(path: String)(implicit logger: Logger): SparkSessionReader[Dataset[Volumes]] = {
    SparkSessionReader { spark =>
      val volumes = Try(ReadWriteUtils
        .readTSV(spark, volumesSchema, path).get) match {
        case Success(df) =>
          df.as[Volumes]
        case Failure(_) =>
          logger.error(s"Failed to read S3 data")
          spark.emptyDataset[Volumes]
      }
      volumes
    }
  }

  /** TODO Read Relevant Competitors
   *
   * @param path s3 uri for Relevant Competitors
   * @return RelevantCompetitors dataset which gives the competitors that are relevant to a given account.
   */
  def readRelevantCompetitorsData(path: String)(implicit logger: Logger): SparkSessionReader[Dataset[RelevantCompetitors]] = {
    SparkSessionReader { spark =>
      val relevant_competitors = Try(ReadWriteUtils
        .readTSV(spark, relevantCompetitorsSchema, path).get) match {
        case Success(df) =>
          df.as[RelevantCompetitors]
        case Failure(_) =>
          logger.error(s"Failed to read S3 data")
          spark.emptyDataset[RelevantCompetitors]
      }
      relevant_competitors
    }
  }

  /** TODO Read Relevant Search Terms
   *
   * @param path s3 uri for Relevant Search Terms
   * @return RelevantSearchTerms dataset which gives The search terms that are relevant to a given account.
   */
  def readRelevantSearchTermsData(path: String)(implicit logger: Logger): SparkSessionReader[Dataset[RelevantSearchTerms]] = {
    SparkSessionReader { spark =>
      val relevant_search_terms = Try(ReadWriteUtils
        .readTSV(spark, relevantSearchTermsSchema, path).get) match {
        case Success(df) =>
          df.as[RelevantSearchTerms]
        case Failure(_) =>
          logger.error(s"Failed to read S3 data")
          spark.emptyDataset[RelevantSearchTerms]
      }
      relevant_search_terms
    }
  }

  /** TODO Calculate Frequency
   *
   * @param competitor_appearances CompetitorAppearances dataset
   * @param scrape_appearances ScrapeAppearances dataset
   * @return Given the CompetitorAppearances & ScrapeAppearances datasets, calculate the frequency of a domain based on the formula: Frequency = appearances / scrape_count.
   */
  def calculateFrequency(competitor_appearances: Dataset[CompetitorAppearances], scrape_appearances: Dataset[ScrapeAppearances])(implicit logger:Logger): SparkSessionReader[DataFrame] =
    SparkSessionReader { spark =>
      import spark.implicits._
      val aggregated_competitor_appearances = competitor_appearances
        .groupBy("date", "search_term", "domain")
        .sum("appearances").as("total_appearances")
        .withColumnRenamed("sum(appearances)", "total_appearances")
        .cache()

      val aggregated_scrape_appearances = scrape_appearances
        .groupBy("date", "search_term")
        .sum("scrape_count")
        .withColumnRenamed("sum(scrape_count)", "total_scrape_count")
        .cache()

      val frequencyStatsDF = aggregated_competitor_appearances
        .join(aggregated_scrape_appearances, Seq("date", "search_term"), "left")
       .withColumn("frequency", $"total_appearances"/$"total_scrape_count")
        .cache()

      frequencyStatsDF
    }

  /** TODO Calculate Impressions
   *
   * @param frequencyStatsDF calculated frequency dataset
   * @param volumes Volumes dataset
   * @return Given the calculated frequency of each domain & Volumes datasets, calculate the impressions each domain has received across the search terms based on the formula: Impressions = Frequency * Daily Volume.
   */
  def calculateImpressions(frequencyStatsDF: DataFrame, volumes: Dataset[Volumes])(implicit logger:Logger): SparkSessionReader[DataFrame] =
    SparkSessionReader { spark =>
      import spark.implicits._
      val aggregated_volumes = volumes
        .groupBy("search_term")
        .sum("volume")
        .withColumnRenamed("sum(volume)", "total_monthly_volume")
        .cache()

      val daily_aggregated_volumes = aggregated_volumes
        .withColumn("daily_volume", $"total_monthly_volume"/30)
        .drop("total_monthly_volume")
        .cache()

      val impressionsStatsDF = frequencyStatsDF
        .join(daily_aggregated_volumes, Seq("search_term"), "left")
        .withColumn("impressions", $"frequency"*$"daily_volume")
        .withColumn("impressions", ceil($"impressions"))
        .drop("total_appearances", "total_scrape_count", "frequency", "daily_volume")
        .cache()

      impressionsStatsDF
    }


  /**  TODO Find the relevant competitors for account_ids in the Relevant Competitors dataset and their impressions across the relevant search terms for the same account_ids across each day.
   *
   * @param relevant_competitors RelevantCompetitors dataset which gives the competitors that are relevant to a given account.
   * @param relevant_search_terms RelevantSearchTerms dataset which gives The search terms that are relevant to a given account.
   * @param impressionsStatsDF calculated impressions dataset
   * @return the Results dataset that contains only the relevant competitors for each account_id and their impressions across the relevant search terms for the same account_id across each day.
   */

  def calculateResult( relevant_competitors: Dataset[RelevantCompetitors], relevant_search_terms: Dataset[RelevantSearchTerms], impressionsStatsDF: DataFrame)(implicit logger:Logger): SparkSessionReader[Dataset[Results]] =
    SparkSessionReader { _ =>
      logger.debug("Calculate Result")
      val impressionsForRelevantCompetitorsDF = relevant_competitors
        .join(impressionsStatsDF, Seq("domain"), "left")
        .cache()

      val relevantImpressionsDF = relevant_search_terms
        .join(impressionsForRelevantCompetitorsDF, Seq("account_id", "search_term"), "left")
        .na.drop(Seq("domain","date"))
        .cache()

      val result = relevantImpressionsDF.groupBy("account_id","date", "domain")
        .sum("impressions")
        .withColumnRenamed("sum(impressions)", "impressions")
        .withColumn("impressions", $"impressions".cast(IntegerType))

      logger.info("Result")
      result.printSchema()
      result.as[Results].cache()
    }

  /** TODO Write final results to a TSV file
   *
   * @param result Results Dataset computed above
   * @param path output path
   */
  def writeResults(result: Dataset[Results], path: String): SparkSessionReader[Unit] =
    SparkSessionReader { _ =>
      ReadWriteUtils.writeTSV(result.orderBy("account_id", "date").toDF(), path)
    }

  def closeSparkSession(): SparkSessionReader[Unit] =
    SparkSessionReader { spark =>
      spark.stop()
    }
}
