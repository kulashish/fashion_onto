package com.jabong.dap.model.customer.data

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{DNDVariables}
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by mubarak on 3/9/15.
 */
object DNDMerger {

  def start(params: ParamInfo, isHistory: Boolean) = {
    println("Start Time: " + TimeUtils.getTodayDate(TimeConstants.DATE_TIME_FORMAT_MS))
    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val saveMode = params.saveMode
    val path = OptionUtils.getOptValue(params.path)
    var paths: Array[String] = new Array[String](2)
    if (null != path) {
      paths = path.split(";")
    }
    val prevDate = OptionUtils.getOptValue(params.fullDate, TimeUtils.getDateAfterNDays(-2, TimeConstants.DATE_FORMAT_FOLDER))

    if (isHistory && null == path && null == OptionUtils.getOptValue(params.fullDate)) {
      println("First full csv path and prev full date both cannot be empty")
    } else {
      val newDate = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)

     }

    if (isHistory) {
      processHistoricalData(incrDate, saveMode)
    }
    println("End Time: " + TimeUtils.getTodayDate(TimeConstants.DATE_TIME_FORMAT_MS))
  }

  /**
   *
   * @param prevDate
   * @param fullcsv
   * @param curDate
   */
  def processData(tablename: String, prevDate: String, curDate: String, filename: String, saveMode: String, deviceType: String, fullcsv: String) {
    var incr: DataFrame = null
    incr = DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.DND, tablename, DataSets.DAILY_MODE, curDate, filename, "true", ";")
    var prevFull: DataFrame = null
    if (null == fullcsv) {
      prevFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.DND, tablename, DataSets.DAILY_MODE, prevDate)
    }
    else {
      prevFull = DataReader.getDataFrame4mCsv(fullcsv, "true", ";")
    }
    val dndFull = mergeDNDData(prevFull, incr)

    var savePath = DataWriter.getWritePath(ConfigConstants.READ_OUTPUT_PATH, DataSets.DND, tablename, DataSets.FULL_MERGE_MODE, curDate)

    DataWriter.writeParquet(dndFull, savePath, saveMode)

  }

  def mergeDNDData(full: DataFrame, newdf: DataFrame): DataFrame = {

    val joined = full.join(newdf, full(DNDVariables.MOBILE_NUMBER) === newdf(DNDVariables.MOBILE_NUMBER), SQL.FULL_OUTER)
      .select(coalesce(full(DNDVariables.MOBILE_NUMBER), newdf(DNDVariables.MOBILE_NUMBER)) as DNDVariables.MOBILE_NUMBER,
        coalesce(full(DNDVariables.EVENT_UUID), newdf(DNDVariables.EVENT_UUID)) as DNDVariables.EVENT_UUID,
          coalesce(full(DNDVariables.EVENT_TYPE_ID), newdf(DNDVariables.EVENT_TYPE_ID)) as DNDVariables.EVENT_TYPE_ID,
          coalesce(full(DNDVariables.ACCOUNT_ID), newdf(DNDVariables.ACCOUNT_ID)) as DNDVariables.ACCOUNT_ID,
          coalesce(full(DNDVariables.LIST_ID), newdf(DNDVariables.LIST_ID)) as DNDVariables.LIST_ID,
          coalesce(full(DNDVariables.RIID), newdf(DNDVariables.RIID)) as DNDVariables.RIID,
          coalesce(full(DNDVariables.EVENT_CAPTURED_DT), newdf(DNDVariables.EVENT_CAPTURED_DT)) as DNDVariables.EVENT_CAPTURED_DT,
          coalesce(full(DNDVariables.CAMPAIGN_ID), newdf(DNDVariables.CAMPAIGN_ID)) as DNDVariables.CAMPAIGN_ID,
          coalesce(full(DNDVariables.LAUNCH_ID), newdf(DNDVariables.LAUNCH_ID)) as DNDVariables.LAUNCH_ID,
          coalesce(full(DNDVariables.PROGRAM_ID), newdf(DNDVariables.PROGRAM_ID)) as DNDVariables.PROGRAM_ID,
          coalesce(full(DNDVariables.AGGREGATOR_ID), newdf(DNDVariables.AGGREGATOR_ID)) as DNDVariables.AGGREGATOR_ID,
          coalesce(full(DNDVariables.COUNTRY_CODE), newdf(DNDVariables.COUNTRY_CODE)) as DNDVariables.COUNTRY_CODE,
          coalesce(full(DNDVariables.MOBILE_CODE), newdf(DNDVariables.MOBILE_CODE)) as DNDVariables.MOBILE_CODE,
          coalesce(full(DNDVariables.MOBILE_CHANNEL), newdf(DNDVariables.MOBILE_CHANNEL)) as DNDVariables.MOBILE_CHANNEL,
          coalesce(full(DNDVariables.MOBILE_CARRIER), newdf(DNDVariables.MOBILE_CARRIER)) as DNDVariables.MOBILE_CARRIER,
          coalesce(full(DNDVariables.SMS_SENT_UUID), newdf(DNDVariables.SMS_SENT_UUID)) as DNDVariables.SMS_SENT_UUID,
          coalesce(full(DNDVariables.DELIVERED_FLAG), newdf(DNDVariables.DELIVERED_FLAG)) as DNDVariables.DELIVERED_FLAG,
          coalesce(full(DNDVariables.AGGREGATOR_MESSAGE_ID), newdf(DNDVariables.AGGREGATOR_MESSAGE_ID)) as DNDVariables.AGGREGATOR_MESSAGE_ID,
          coalesce(full(DNDVariables.AGGREGATOR_MESSAGE_SUBID), newdf(DNDVariables.AGGREGATOR_MESSAGE_SUBID)) as DNDVariables.AGGREGATOR_MESSAGE_SUBID,
          coalesce(full(DNDVariables.AGGREGATOR_STATUS_CODE), newdf(DNDVariables.AGGREGATOR_STATUS_CODE)) as DNDVariables.AGGREGATOR_STATUS_CODE,
          coalesce(full(DNDVariables.AGGREGATOR_STATUS_DESC), newdf(DNDVariables.AGGREGATOR_STATUS_DESC)) as DNDVariables.AGGREGATOR_STATUS_DESC
      )
    return joined
  }

  def processHistoricalData(minDate: String, saveMode: String) {

  }

}
