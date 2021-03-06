package com.jabong.dap.data.read

import java.io.File

import com.jabong.dap.common.Spark
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.DataVerifier
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

object DataReader extends Logging {

  /**
   * Method to read raw HDFS data for a source, table and a given date and get a dataFrame for the same.
   * WARNING: Throws DataNotFound exception if data is not found.
   * WARNING: Throws ValidFormatNotFound exception if suitable format is not found.
   */
  def getDataFrame(basePath: String, source: String, tableName: String, mode: String, date: String): DataFrame = {

    require(source != null, "Source Type is null")
    require(tableName != null, "Table Name is null")
    require(mode != null, "Mode is null")
    require(date != null, "Date is null")

    try {
      fetchDataFrame(basePath, source, tableName, mode, date)
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
   * Method to read raw HDFS data for a source, table and a given date and get a dataFrame for the same.
   * WARNING: Does not Throw DataNotFound exception instead return null data frame.
   * WARNING: Throws ValidFormatNotFound exception if suitable format is not found.
   */
  def getDataFrameOrNull(basePath: String, source: String, tableName: String, mode: String, date: String): DataFrame = {
    try {
      getDataFrame(basePath, source, tableName, mode, date)
    } catch {
      case e: DataNotFound =>
        logger.error("Data not found for the date")
        null
      case e: ValidFormatNotFound =>
        logger.error("Format could not be resolved for the given files in directory")
        throw new ValidFormatNotFound
      case e: Throwable =>
        logger.error("Some other Error found: " + e.printStackTrace)
        null
    }
  }

  /**
   * Method to read raw HDFS data for a source and table and get a dataFrame for the same. Checks
   * for the data for today's date. In case data is not found, checks for the data for yesterday's
   * date.
   * WARNING: Throws DataNotFound exception if data is not found for today and yesterday.
   * WARNING: Throws ValidFormatNotFound exception if suitable format is not found.
   */
  def getDataFrame(basePath: String, source: String, tableName: String, mode: String): DataFrame = {
    require(source != null, "Source Type is null")
    require(tableName != null, "Table Name is null")
    require(mode != null, "Mode is null")

    try {
      fetchDataFrame(basePath, source, tableName, mode, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT))
    } catch {
      case e: DataNotFound =>
        logger.info("Data not found for the table " + tableName + " and source for yesterday's data.")
        throw new DataNotFound
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
  private def fetchDataFrame(basePath: String, source: String, tableName: String, mode: String, date: String): DataFrame = {
    var diskMode = mode
    var reqDate: String = null

    if (mode.equals(DataSets.DAILY_MODE) || mode.equals(DataSets.MONTHLY_MODE)) {
      reqDate = date
    } else if (mode.equals(DataSets.FULL_MERGE_MODE)) {
      diskMode = DataSets.FULL
      reqDate = "%s-%s".format(date, "24")
    } else if (mode.equals(DataSets.FULL_FETCH_MODE)) {
      // scan next day data
      diskMode = DataSets.FULL
      val nextDay = TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT, date)
      reqDate = DateResolver.getDateWithHour(basePath, source, tableName, diskMode, nextDay)
    } else {
      reqDate = date
    }

    val fetchPath = PathBuilder.buildPath(basePath, source, tableName, diskMode, reqDate)
    logger.info(fetchPath)
    val saveFormat = FormatResolver.getFormat(fetchPath)
    val context = Spark.getContext(saveFormat)
    if (saveFormat.equals(DataSets.CSV)) {
      context.read.format("com.databricks.spark.csv").load(fetchPath)
    } else {
      logger.info("Reading data from hdfs: " + fetchPath + " in format " + saveFormat)
      if (DataVerifier.dataExists(fetchPath))
        context.read.format(saveFormat).load(fetchPath)
      else
        throw new DataNotFound
    }
  }

  def getDataFrame4mCsv(basePath: String, source: String, tableName: String, mode: String, date: String, filename: String, header: String, delimiter: String): DataFrame = {
    require(basePath != null, "Base Path is null")
    require(source != null, "Source Type is null")
    require(tableName != null, "Table Name is null")
    require(mode != null, "Mode is null")
    require(date != null, "Date is null")

    val fetchPath = PathBuilder.buildPath(basePath, source, tableName, mode, date)
    logger.info("Reading data from hdfs: " + fetchPath + File.separator + filename + " in csv format")

    if (DataVerifier.dataExists(fetchPath, filename))
      Spark.getSqlContext().read.format("com.databricks.spark.csv").option("header", header).option("delimiter", delimiter).load(fetchPath)
    else {
      logger.error("Data not found for the path %s and filename %s".format(fetchPath, filename))
      throw new DataNotFound
    }
  }

  def getDataFrame4mCsvOrNull(basePath: String, source: String, tableName: String, mode: String, date: String, filename: String, header: String, delimiter: String): DataFrame = {
    require(basePath != null, "Base Path is null")
    require(source != null, "Source Type is null")
    require(tableName != null, "Table Name is null")
    require(mode != null, "Mode is null")
    require(date != null, "Date is null")

    val fetchPath = PathBuilder.buildPath(basePath, source, tableName, mode, date)
    logger.info("Reading data from hdfs: " + fetchPath + File.separator + filename + " in csv format")
    try {
      if (DataVerifier.dataExists(fetchPath, filename))
        Spark.getSqlContext().read.format("com.databricks.spark.csv").option("header", header).option("delimiter", delimiter).load(fetchPath)
      else {
        logger.error("Data not found for the path %s and filename %s".format(fetchPath, filename))
        null
      }
    } catch {
      case e: DataNotFound =>
        logger.info("Data not found for the path %s and filename %s".format(fetchPath, filename))
        null
      case e: UnsupportedOperationException =>
        if ("empty collection".equalsIgnoreCase(e.getMessage)) {
          logger.info("Empty file for the path %s and filename %s".format(fetchPath, filename))
          null
        } else {
          throw e
        }
    }
  }

  def getDataFrame4mCsv(fullCSVPath: String, header: String, delimiter: String): DataFrame = {
    require(fullCSVPath != null, "Path is null")

    logger.info("Reading data from hdfs: " + fullCSVPath + " in csv format")
    // used dir exists as the path itself contains the filename as well.
    if (DataVerifier.dirExists(fullCSVPath))
      Spark.getSqlContext().read.format("com.databricks.spark.csv").option("header", header).option("delimiter", delimiter).load(fullCSVPath)
    else {
      logger.error("Data not found for " + fullCSVPath)
      throw new DataNotFound
    }
  }

  def getDataFrame4mFullPath(fullPath: String, format: String): DataFrame = {
    require(fullPath != null, "Path is null")

    logger.info("Reading data from hdfs: " + fullPath + " in " + format + " format")
    // used dir exists as the path itself contains the filename as well.
    if (DataVerifier.dataExists(fullPath))
      Spark.getSqlContext().read.format(format).load(fullPath)
    else {
      logger.error("Data not found for " + fullPath)
      throw new DataNotFound
    }
  }

}
