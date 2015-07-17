package com.jabong.dap.data.read

import com.jabong.dap.common.Spark
import com.jabong.dap.common.time.{ Constants, TimeUtils }
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

object DataReader extends Logging {

  /**
   * Method to read raw HDFS data for a source and table and get a dataFrame for the same.
   * WARNING: Throws DataNotFound exception if data is not found.
   * WARNING: Throws ValidFormatNotFound exception if suitable format is not found.
   */

  def getDataFrame(source: String, tableName: String, dataType: String, date: String): DataFrame = {
    require(source != null, "Source Type is null")
    require(tableName != null, "Table Name is null")
    require(dataType != null, "Data Type is null")
    require(date != null, "Date is null")

    try {
      val dateWithHour = "%s-%s".format(date, DateResolver.getDateHour(source, tableName, dataType, date))
      val fetchPath = PathBuilder.buildPath(source, tableName, dataType, dateWithHour)
      val saveFormat = FormatResolver.getFormat(fetchPath)
      val context = getContext(saveFormat)
      context.read.format(saveFormat).load(fetchPath)
    } catch {
      case e: DataNotFound =>
        logger.error("Data not found for the date")
        throw new DataNotFound
      case e: ValidFormatNotFound =>
        logger.error("Format could not be resolved for the given files in directory")
        throw new ValidFormatNotFound
    }
  }

  def getDataFrame(source: String, tableName: String, dataType: String): DataFrame = {
    val date = TimeUtils.getTodayDate(Constants.DATE_FORMAT)
    getDataFrame(source, tableName, dataType, date)
  }

  def getContext(saveFormat: String) = saveFormat match {
    case "parquet" => Spark.getSqlContext()
    case "orc" => Spark.getHiveContext()
    case _ => null
  }

}
