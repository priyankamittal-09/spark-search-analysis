package com.data.model

import java.sql.Date

case class CompetitorAppearances(
                                  date: Date,
                                  device: String,
                                  search_term: String,
                                  domain: String,
                                  appearances: Int,
                                )
