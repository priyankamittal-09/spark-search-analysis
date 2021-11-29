package com.data.utils

import java.time.LocalDate
import java.time.temporal.ChronoUnit

object DateUtils {

  def generateDatesInRange(startDateStr: String, endDateStr:String): Seq[LocalDate] ={
    val start = LocalDate.parse(startDateStr)
    val end = LocalDate.parse(endDateStr)

    for(daysPlus <- 0 to ChronoUnit.DAYS.between(start,end).toInt)
      yield start.plusDays(daysPlus)
  }
}
