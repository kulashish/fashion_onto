package com.jabong.dap.data.read

import com.jabong.dap.common.Spark
import com.jabong.dap.common.time.{ Constants, TimeUtils }
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

object DataReader extends Logging {

  /**
   * Method to read raw HDFS data for a source, table and a given date and get a dataFrame for the same.
   * WARNING: Throws DataNotFound exception if data is not found.
   * WARNING: Throws ValidFormatNotFound exception if suitable format is not found.
   */
  def getDataFrame(source: String, tableName: String, mode: String, date: String): DataFrame = {
    require(source != null, "Source Type is null")
    require(tableName != null, "Table Name is null")
    require(mode != null, "Mode is null")
    require(date != null, "Date is null")

    try {
      fetchDataFrame(source, tableName, mode, date)
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
  def getDataFrame(source: String, tableName: String, mode: String): DataFrame = {
    require(source != null, "Source Type is null")
    require(tableName != null, "Table Name is null")
    require(mode != null, "Mode is null")

    try {
      fetchDataFrame(source, tableName, mode, TimeUtils.getTodayDate(Constants.DATE_FORMAT))
    } catch {
      case e: DataNotFound =>
        logger.info("Data not found for the given table and source for today's date. Trying to fetch for yesterday's data.")
        try {
          fetchDataFrame(source, tableName, mode, TimeUtils.getDateAfterNDays(-1, Constants.DATE_FORMAT))
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
  private def fetchDataFrame(source: String, tableName: String, mode: String, date: String): DataFrame = {
    val dateWithHour = DateResolver.getDateWithHour(source, tableName, mode, date)
    val fetchPath = PathBuilder.buildPath(source, tableName, mode, dateWithHour)
    println(fetchPath)
    logger.info(fetchPath)
    val saveFormat = FormatResolver.getFormat(fetchPath)
    val context = getContext(saveFormat)
    context.read.format(saveFormat).load(fetchPath)
  }

  /**
   * Gets the spark context for a given format
   */
  def getContext(saveFormat: String) = saveFormat match {
    case "parquet" => Spark.getSqlContext()
    case "orc" => Spark.getHiveContext()
    case _ => null
  }

}
