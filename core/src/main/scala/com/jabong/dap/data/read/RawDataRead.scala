package com.jabong.dap.data.read

import com.jabong.dap.common.Spark
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

object RawDataRead extends Logging {

  /**
   * Method to read raw HDFS data for a source and table and get a dataFrame for the same.
   * WARNING: Throws DataNotFound exception if data is not found.
   */
  def getDataFrame (source: String, tableName: String, dataType: String, date: String): DataFrame = {
    require(source != null, "Source Type is null")
    require(tableName != null, "Table Name is null")
    require(dataType != null, "Data Type is null")


    val pathDate = DateResolver.resolveDate(date, dataType)
    try {
      val dateHour = DateResolver.getDateHour(source, tableName, dataType, pathDate)
      val pathDateWithHour = "%s-%s".format(pathDate, dateHour)

      val fetchPath = PathBuilder.buildPath(source, tableName, dataType, pathDateWithHour)
      val fileFormat = FormatResolver.resolveFormat(fetchPath)
      val context = getContext(fileFormat)
      context.read.format(fileFormat).load(fetchPath)
    } catch {
      case e : DataNotFound =>
        logger.error("Data not found for the date")
        throw new DataNotFound
    }
  }

  def getContext(saveFormat: String) = saveFormat match {
    case "parquet" => Spark.getSqlContext()
    case "orc" => Spark.getHiveContext()
    case _ => null
  }


}
