package com.data.utils

import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

object ReadWriteUtils {

  def readTSV(spark: SparkSession, schema: StructType, path: String)(implicit logger: Logger): Option[DataFrame] = {
    val df = Try(spark
      .sqlContext
      .read
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .schema(schema)
      .load(path)
      .cache()) match {
      case Success(content) => Some(content)
      case Failure(exception) =>
        logger.error(exception.printStackTrace().toString)
        None
    }
    df
  }


  def writeTSV(df: DataFrame, path: String)(implicit logger: Logger): Unit = {
    df.coalesce(1)
      .write
      .format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .mode("overwrite")
      .save(path)
    df.show()
    logger.info(s"TSV file written at $path")
  }

}
