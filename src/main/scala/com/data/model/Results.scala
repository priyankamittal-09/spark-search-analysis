package com.data.model

import java.sql.Date

case class Results(
                    account_id: Int,
                    date: Date,
                    domain: String,
                    impressions: Int,
                  )
