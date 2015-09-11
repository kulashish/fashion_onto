package com.jabong.dap.model.customer.data

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.DNDVariables
import com.jabong.dap.common.time.{TimeUtils, TimeConstants}
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
    println("Start Time: " + TimeUtils.getTodayDate(TimeConstants.DATE_TIME_FORMAT_MS))
    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getTodayDate(TimeConstants.DATE_FORMAT_FOLDER))
    val saveMode = params.saveMode
    val path = OptionUtils.getOptValue(params.path)
    var filename = "53699_SMS_OPT_OUT_" + TimeUtils.getTodayDate("YYYYMMDD") + "*"

    val prevDate = OptionUtils.getOptValue(params.fullDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))

    if ( null == path && null == OptionUtils.getOptValue(params.fullDate)) {
      println("First full csv path and prev full date both cannot be empty")

      processData(DataSets.SMS_OPT_OUT, prevDate, incrDate, filename, saveMode, path)
    } else {
      val newDate = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    }
  }

  /**
   *
   * @param prevDate
   * @param fullcsv
   * @param curDate
   */
  def processData(tablename: String, prevDate: String, curDate: String, filename: String, saveMode: String, fullcsv: String) {
    var incr: DataFrame = null
    incr = DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, tablename, DataSets.DAILY_MODE, curDate, filename, "true", ";")
    var prevFull: DataFrame = null
    if (null == fullcsv) {
      prevFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.RESPONSYS, tablename, DataSets.FULL, prevDate)
    }
    else {
      prevFull = DataReader.getDataFrame4mCsv(fullcsv, "true", ",")
    }
    if(null == incr || incr.count().equals(0)){
      var savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.RESPONSYS, tablename, DataSets.FULL, curDate)

      DataWriter.writeParquet(prevFull, savePath, saveMode)

      return
    }
    val smsOptOutFull = mergeData(prevFull, incr)

    var savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.RESPONSYS, tablename, DataSets.FULL, curDate)

    DataWriter.writeParquet(smsOptOutFull, savePath, saveMode)

  }

  def mergeData(full: DataFrame, newdf: DataFrame): DataFrame = {

    val joined = full.join(newdf, full(DNDVariables.MOBILE_NUMBER) === newdf(DNDVariables.MOBILE_NUMBER), SQL.FULL_OUTER)
      .select(coalesce(full(DNDVariables.MOBILE_NUMBER), newdf(DNDVariables.MOBILE_NUMBER)) as DNDVariables.MOBILE_NUMBER,
        coalesce(full(DNDVariables.PROCESSED_DATE), newdf(DNDVariables.EVENT_CAPTURED_DT)) as DNDVariables.PROCESSED_DATE
      )
    return joined
  }

}
