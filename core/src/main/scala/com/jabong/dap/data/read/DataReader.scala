package com.jabong.dap.data.read

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.variables.CustomerVariables
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.storage.DataSets
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
      context.read.format(saveFormat).load(fetchPath)
    }
  }

  def tokenize(x:String, delimiter: String):(String,String,String,String,String)={
    val t =x.split(delimiter)
    val a = scala.collection.mutable.ListBuffer[String]()
    t.foreach(x => a.+=:(x))
    if (a.length == 3){
      return (a(2),a(1),a(0),null,null)
    }
    return (a(4),a(3),a(2),a(1),a(0))
  }

  def getDataFrameCsv4mDCF(path: String, delimeter: String): DataFrame = {
    require(path != null, "Mode is null")
    require(delimeter != null, "Date is null")

    try {
      val csv = Spark.getContext().textFile(path)
      val x= csv.map(x =>  tokenize(x, delimeter))
      val df2 = Spark.getSqlContext().createDataFrame(x).
        withColumnRenamed("_1",CustomerVariables.RESPONSYS_ID).
        withColumnRenamed("_2",CustomerVariables.ID_CUSTOMER).
        withColumnRenamed("_3",CustomerVariables.EMAIL).
        withColumnRenamed("_4",CustomerVariables.BROWSER_ID).
        withColumnRenamed("_5",CustomerVariables.DOMAIN)
      df2
    } catch {
      case e: DataNotFound =>
        logger.error("Data not found for the date")
        throw new DataNotFound
      case e: ValidFormatNotFound =>
        logger.error("Format could not be resolved for the given files in directory")
        throw new ValidFormatNotFound
    }
  }

}
