package com.data

import com.typesafe.config.Config

class Settings(config: Config) extends Serializable {
  val scrapeAppearancesDataPath: String = config.getString("input.scrape_appearances")
  val competitorAppearancesDataPath: String = config.getString("input.competitor_appearances")
  val volumesDataPath: String = config.getString("input.volumes")
  val relevantCompetitorsDataPath: String = config.getString("input.relevant_competitors")
  val relevantSearchTermsDataPath: String = config.getString("input.relevant_search_terms")
  val resultPath: String = config.getString("output.result")
  val s3Endpoint:String = config.getString("s3-endpoint")
  val awsAccessKey:String = config.getString("access-key")
  val awsSecretKey:String = config.getString("secret-key")
  val awsCertificateCheck:String = config.getString("cert-check")
}
