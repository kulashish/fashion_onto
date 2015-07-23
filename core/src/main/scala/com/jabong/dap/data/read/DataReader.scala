package com.jabong.dap.data.read

import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.Spark
import com.jabong.dap.data.storage.DataSets
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

object DataReader extends Logging {

  /**
   * Method to read raw HDFS data for a source, table and a given date and get a dataFrame for the same.
   * WARNING: Throws DataNotFound exception if data is not found.
   * WARNING: Throws ValidFormatNotFound exception if suitable format is not found.
   */
  def getDataFrame(basePath: String, source: String, tableName: String, mode: String, date: String, header: Boolean): DataFrame = {

    require(source != null, "Source Type is null")
    require(tableName != null, "Table Name is null")
    require(mode != null, "Mode is null")
    require(date != null, "Date is null")

    try {
      fetchDataFrame(basePath, source, tableName, mode, date, header)
    } catch {
      case e: DataNotFound =>
        logger.error("Data not found for the date")
        throw new DataNotFound
      case e: ValidFormatNotFound =>
        logger.error("Format could not be resolved for the given files in directory")
        throw new ValidFormatNotFound
    }
  }

  /**
   * Method to read raw HDFS data for a source and table and get a dataFrame for the same. Checks
   * for the data for today's date. In case data is not found, checks for the data for yesterday's
   * date.
   * WARNING: Throws DataNotFound exception if data is not found for today and yesterday.
   * WARNING: Throws ValidFormatNotFound exception if suitable format is not found.
   */
  def getDataFrame(basePath: String, source: String, tableName: String, mode: String, header: Boolean): DataFrame = {
    require(source != null, "Source Type is null")
    require(tableName != null, "Table Name is null")
    require(mode != null, "Mode is null")

    try {
      fetchDataFrame(basePath, source, tableName, mode, TimeUtils.getTodayDate(TimeConstants.DATE_FORMAT_FOLDER), header)
    } catch {
      case e: DataNotFound =>
        logger.info("Data not found for the given table and source for today's date. Trying to fetch for yesterday's data.")
        try {
          fetchDataFrame(basePath, source, tableName, mode, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER), header)
        } catch {
          case e: DataNotFound =>
            logger.error("Data not found for the given table and source for yesterday's date")
            throw new DataNotFound
        }
      case e: ValidFormatNotFound =>
        logger.error("Format could not be resolved for the given files in directory")
        throw new ValidFormatNotFound
    }
  }

  /**
   * Private function to read raw HDFS data for a source, table and a given date and get a dataFrame for the same.
   * WARNING: Throws DataNotFound exception if data is not found.
   * WARNING: Throws ValidFormatNotFound exception if suitable format is not found.
   */
  private def fetchDataFrame(basePath: String, source: String, tableName: String, mode: String, date: String, header: Boolean): DataFrame = {
    val dateWithHour = DateResolver.getDateWithHour(basePath, source, tableName, mode, date)
    val fetchPath = PathBuilder.buildPath(basePath, source, tableName, mode, dateWithHour)
    val saveFormat = FormatResolver.getFormat(fetchPath)
    val context = Spark.getContext(saveFormat)
    if (saveFormat.equals(DataSets.CSV)) {
      context.read.format("com.databricks.spark.csv").option("header", header.toString).load(fetchPath)
    } else {
      context.read.format(saveFormat).load(fetchPath)
    }
  }

}
