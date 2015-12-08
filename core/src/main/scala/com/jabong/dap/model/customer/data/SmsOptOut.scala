package com.jabong.dap.model.customer.data

import java.io.File

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.DNDVariables
import com.jabong.dap.common.time.{ TimeUtils, TimeConstants }
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by mubarak on 9/9/15.
 */
object SmsOptOut {

  def start(params: ParamInfo) = {
    //println("Start Time: " + TimeUtils.getTodayDate(TimeConstants.DATE_TIME_FORMAT_MS))
    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getTodayDate(TimeConstants.DATE_FORMAT_FOLDER))
    val saveMode = params.saveMode
    val path = OptionUtils.getOptValue(params.path)
    var paths: Array[String] = new Array[String](2)
    if (null != path) {
      paths = path.split(";")
    }
    val prevDate = OptionUtils.getOptValue(params.fullDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    processDataResponsys(DataSets.SMS_OPT_OUT, prevDate, incrDate, saveMode, paths(0))
    processDataSolutionsInfinity(prevDate, incrDate, saveMode, paths(1))
  }

  /**
   *
   * @param prevDate
   * @param fullcsv
   * @param incrDate
   */
  def processDataResponsys(tablename: String, prevDate: String, incrDate: String, saveMode: String, fullcsv: String) {
    // As this is just being merged here and then later used as input to other files.
    val savePath = DataWriter.getWritePath(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, tablename, DataSets.FULL_MERGE_MODE, incrDate)
    if (DataWriter.canWrite(saveMode, savePath)) {
      var prevFull: DataFrame = null
      if (null == fullcsv) {
        prevFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, tablename, DataSets.FULL_MERGE_MODE, prevDate)
      } else {
        prevFull = DataReader.getDataFrame4mCsv(fullcsv.trim(), "true", ",").withColumnRenamed("MOBILE", DNDVariables.MOBILE_NUMBER)
      }
      val newDate = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
      val filename = "53699_SMS_OPT_OUT_" + newDate + ".txt"
      val incr = DataReader.getDataFrame4mCsvOrNull(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, tablename, DataSets.DAILY_MODE, incrDate, filename, "true", ";")
      if (null == incr || incr.count().equals(0)) {
        DataWriter.writeParquet(prevFull, savePath, saveMode)
        return
      }
      val smsOptOutFull = mergeData(prevFull, incr)
      DataWriter.writeParquet(smsOptOutFull, savePath, saveMode)
    }

  }

  /**
   *
   * @param prevDate
   * @param fullcsvPath
   * @param incrDate
   */
  def processDataSolutionsInfinity(prevDate: String, incrDate: String, saveMode: String, fullcsvPath: String) {
    // As here we just doing a merge and after merge we just write on the INPUT_PATH
    val savePath = DataWriter.getWritePath(ConfigConstants.INPUT_PATH, DataSets.SOLUTIONS_INFINITI, DataSets.BLOCK_LIST_NUMBERS, DataSets.FULL_MERGE_MODE, incrDate)
    val newDate = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    val filename1 = "blocklist_numbers_jabong" + newDate + ".csv"
    val filename2 = "blocklist_numbers_jabongdnd" + newDate + ".csv"
    if (DataWriter.canWrite(saveMode, savePath)) {
      var prevFull: DataFrame = null
      if (null == fullcsvPath) {
        prevFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.SOLUTIONS_INFINITI, DataSets.BLOCK_LIST_NUMBERS, DataSets.FULL_MERGE_MODE, prevDate)
      } else {
        prevFull = DataReader.getDataFrame4mCsv(fullcsvPath.trim() + File.separator + "blocklist_numbers_jabong.csv", "true", ",").unionAll(DataReader.getDataFrame4mCsv(fullcsvPath.trim() + File.separator + "blocklist_numbers_jabongdnd.csv", "true", ",")).dropDuplicates()
      }
      // as we may not get files everyday.
      val incrjb = DataReader.getDataFrame4mCsvOrNull(ConfigConstants.INPUT_PATH, DataSets.SOLUTIONS_INFINITI, DataSets.BLOCK_LIST_NUMBERS, DataSets.DAILY_MODE, incrDate, filename1, "true", ";")
      val incrdnd = DataReader.getDataFrame4mCsvOrNull(ConfigConstants.INPUT_PATH, DataSets.SOLUTIONS_INFINITI, DataSets.BLOCK_LIST_NUMBERS, DataSets.DAILY_MODE, incrDate, filename2, "true", ";")
      var incr: DataFrame = null

      if (incrjb != null && incrdnd != null) {
        incr = incrdnd.unionAll(incrjb).dropDuplicates()
      } else if (incrjb == null && incrdnd != null) {
        incr = incrdnd
      } else if (incrjb != null && incrdnd == null) {
        incr = incrjb
      }

      if (null == incr || incr.count().equals(0)) {
        if (!prevFull.columns.contains(DNDVariables.PROCESSED_DATE)) {
          prevFull = prevFull.withColumn(DNDVariables.PROCESSED_DATE, lit(TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_TIME_FORMAT)))
        }
        DataWriter.writeParquet(prevFull, savePath, saveMode)
        return
      }
      val smsOptOutFull = mergeSolotionsData(prevFull, incr)
      DataWriter.writeParquet(smsOptOutFull, savePath, saveMode)
    }

  }
  def mergeData(full: DataFrame, newdf: DataFrame): DataFrame = {
    val joined = full.join(newdf, full(DNDVariables.MOBILE_NUMBER) === newdf(DNDVariables.MOBILE_NUMBER), SQL.FULL_OUTER)
      .select(coalesce(full(DNDVariables.MOBILE_NUMBER), newdf(DNDVariables.MOBILE_NUMBER)) as DNDVariables.MOBILE_NUMBER,
        coalesce(full(DNDVariables.PROCESSED_DATE), newdf(DNDVariables.EVENT_CAPTURED_DT)) as DNDVariables.PROCESSED_DATE
      )
    return joined
  }

  def mergeSolotionsData(full: DataFrame, newdf: DataFrame): DataFrame = {
    val joined = full.join(newdf, full(DNDVariables.MOBILE_NUMBER) === newdf(DNDVariables.MOBILE_NUMBER), SQL.FULL_OUTER)
      .select(coalesce(full(DNDVariables.MOBILE_NUMBER), newdf(DNDVariables.MOBILE_NUMBER)) as DNDVariables.MOBILE_NUMBER,
        coalesce(full(DNDVariables.PROCESSED_DATE), lit(TimeUtils.getTodayDate(TimeConstants.DATE_TIME_FORMAT))) as DNDVariables.PROCESSED_DATE
      )
    return joined
  }

}
