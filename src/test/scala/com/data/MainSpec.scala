package com.data

import java.sql.Date
import com.data.model.{CompetitorAppearances, RelevantCompetitors, RelevantSearchTerms, Results, ScrapeAppearances, Volumes}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.specs2.Specification
import org.specs2.matcher.MatchResult
import org.specs2.specification.AfterAll
import org.specs2.specification.core.SpecStructure

import scala.collection.JavaConverters._
import scala.collection.mutable

class MainSpec extends Specification with AfterAll {
  def is: SpecStructure = sequential ^
    s2"""
      search data analysis
      =============

        Test Case 1 Passed $calculateFrequencyTest

        Test Case 2 Passed $calculateImpressionsTest

        Test Case 3 Passed $calculateResultTest

    """

  Logger.getLogger("org").setLevel(Level.ERROR)
  implicit val logger: Logger = Logger.getLogger(this.getClass)
  val spark: SparkSession = SparkSession
    .builder
    .appName("MainSpec")
    .master("local[*]")
    .getOrCreate()

  logger.info("Test Cases Started")

  def afterAll: Unit = {
    spark.stop()
  }

  import spark.implicits._

  val sample_scrape_appearances: Dataset[ScrapeAppearances] =
    List(
        ScrapeAppearances(Date.valueOf("2021-11-06"), "desktop", "ads on facebook", 3),
        ScrapeAppearances(Date.valueOf("2021-11-06"), "desktop", "keyword planner", 23)
  ).toDS()

  val sample_competitor_appearances: Dataset[CompetitorAppearances] =
    List(
        CompetitorAppearances(Date.valueOf("2021-11-06"), "mobile", "ads on facebook", "linkedin.com", 2),
        CompetitorAppearances(Date.valueOf("2021-11-06"), "desktop", "ads on facebook", "facebook.com", 3)
    ).toDS()

  val sample_volumes: Dataset[Volumes] =
    List(
      Volumes("google keyword planning", "mobile", 6050),
      Volumes("ads on facebook", "desktop", 7400)
    ).toDS()

  val sample_relevant_competitors: Dataset[RelevantCompetitors] =
    List(
      RelevantCompetitors(3, "linkedin.com"),
      RelevantCompetitors(2, "algebradigital.co.uk"),
      RelevantCompetitors(1, "opteo.com"),
      RelevantCompetitors(1, "facebook.com")
    ).toDS()

  val sample_relevant_search_terms: Dataset[RelevantSearchTerms] =
    List(
      RelevantSearchTerms(3, "ads on facebook"),
      RelevantSearchTerms(1, "google ads"),
      RelevantSearchTerms(2, "google ads")
    ).toDS()

  val sampleFrequencyStatsDF: DataFrame = Seq(
    (Date.valueOf("2021-11-06"), "ads on facebook", "facebook.com", 3, 3, 1.0),
    (Date.valueOf("2021-11-06"), "ads on facebook", "linkedin.com", 2, 3, 0.6666666666666666)
  )
    .toDF("date", "search_term", "domain", "total_appearances", "total_scrape_count", "frequency")

  val sampleImpressionsStatsDF: DataFrame = Seq(
    ("ads on facebook", Date.valueOf("2021-11-06"), "facebook.com", 247 ),
    ("ads on facebook", Date.valueOf("2021-11-06"), "linkedin.com", 165 )
  )
    .toDF("search_term", "date", "domain", "impressions")

  val sample_results: Dataset[Results] =
    List(
      Results(3, Date.valueOf("2021-11-06"), "linkedin.com", 165)
    ).toDS()

  def calculateFrequencyTest: MatchResult[mutable.Buffer[Row]] = {
    logger.info("Test Case 1: Given the sample CompetitorAppearances & sample ScrapeAppearances datasets, the frequency of the given domain should be calculated correctly")
    val calculatedDF = Main.calculateFrequency(sample_competitor_appearances, sample_scrape_appearances).run(spark)
    logger.info("Sample Competitor Appearances")
    sample_competitor_appearances.show()
    logger.info("Sample Scrape Appearances")
    sample_scrape_appearances.show()
    val calculated = calculatedDF.collectAsList()
    val expected = sampleFrequencyStatsDF.collectAsList()
    logger.info(s"Calculated: $calculated")
    logger.info(s"Expected: $expected")
    calculated.asScala must beEqualTo(expected.asScala)
  }

    def calculateImpressionsTest: MatchResult[mutable.Buffer[Row]] = {
      println("Test Case 2: Given the calculated frequency of each domain & sample Volumes datasets, the impressions each domain has received across the search terms should be calculated correctly")
      val calculatedDF = Main.calculateImpressions(sampleFrequencyStatsDF, sample_volumes).run(spark)
      logger.info("Sample Frequency Stats")
      sampleFrequencyStatsDF.show()
      logger.info("Sample Volumes Data")
      sample_volumes.show()
      val calculated = calculatedDF.collectAsList()
      val expected = sampleImpressionsStatsDF.collectAsList()
      logger.info(s"Calculated: $calculated")
      logger.info(s"Expected: $expected")
      calculated.asScala must beEqualTo(expected.asScala)
    }

    def calculateResultTest: MatchResult[mutable.Buffer[Results]] = {
      println("Test Case 3: Given the impressions each domain has received, relevant search terms for each account id & relevant competitors for each account id, the final result should contains only the relevant competitors for each account_id and their impressions across the relevant search terms for the same account_id across each day")
      val calculatedDF = Main.calculateResult(sample_relevant_competitors, sample_relevant_search_terms, sampleImpressionsStatsDF).run(spark)
      logger.info("Sample Relevant Competitors")
      sample_relevant_competitors.show()
      logger.info("Sample Relevant Search Terms")
      sample_relevant_search_terms.show()
      logger.info("Sample Impressions Stats")
      sampleImpressionsStatsDF.show()
      val calculated = calculatedDF.collectAsList()
      val expected = sample_results.collectAsList()
      logger.info(s"Calculated: $calculated")
      logger.info(s"Expected: $expected")
      calculated.asScala must beEqualTo(expected.asScala)
    }

}

