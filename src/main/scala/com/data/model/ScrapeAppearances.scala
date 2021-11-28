package com.data.model

import java.sql.Date

case class ScrapeAppearances(
                              date: Date,
                              device: String,
                              search_term: String,
                              scrape_count: Int,
                            )
