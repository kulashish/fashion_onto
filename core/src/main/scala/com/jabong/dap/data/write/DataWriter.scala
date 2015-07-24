package com.jabong.dap.data.write

import com.jabong.dap.data.read.PathBuilder
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * Created by pooja on 23/7/15.
 */
object DataWriter extends Logging {
  /**
   *
   * @param df
   * @param basePath
   * @param source
   * @param tableName
   * @param mode
   * @param date
   */
  def writeCsv(df: DataFrame, basePath: String, source: String, tableName: String, mode: String, date: String) {
    val writePath = PathBuilder.buildPath(basePath, source, tableName, mode, date)
    df.write.format("com.databricks.spark.csv").option("delimiter", ";").save(writePath)
  }

  /**
   *
   * @param df
   * @param basePath
   * @param source
   * @param tableName
   * @param mode
   * @param date
   */
  def writeParquet(df: DataFrame, basePath: String, source: String, tableName: String, mode: String, date: String) {
    val writePath = PathBuilder.buildPath(basePath, source, tableName, mode, date)
    df.write.parquet(writePath)
  }

}
