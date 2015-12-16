package com.jabong.dap.model.dataFeeds

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.acq.common.{ ParamJobConfig, ParamInfo }
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.HashMap

/**
 * Created by pooja on 30/10/15.
 */
abstract class DataFeedsModel {

  def start(params: ParamInfo) = {
    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.YESTERDAY_FOLDER)
    val saveMode = params.saveMode
    val paths = OptionUtils.getOptValue(params.path)
    val prevDate = OptionUtils.getOptValue(params.fullDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER, incrDate))
    val isHistory = OptionUtils.getOptBoolVal(ParamJobConfig.paramJobInfo.isHistory)

    if (true == isHistory) {
      val days = TimeUtils.daysFromToday(incrDate, TimeConstants.DATE_FORMAT_FOLDER)
      for (day <- days to 1 by -1) {
        val incr = TimeUtils.getDateAfterNDays(-day, TimeConstants.DATE_FORMAT_FOLDER)
        val prev = TimeUtils.getDateAfterNDays(-(day + 1), TimeConstants.DATE_FORMAT_FOLDER)
        if (canProcess(incr, saveMode)) {
          val dfMap = readDF(incr, prev, paths)
          val dfWriteMap = process(dfMap)
          write(dfWriteMap, saveMode, incr)
        }
      }
    } else {
      if (canProcess(incrDate, saveMode)) {
        val dfMap = readDF(incrDate, prevDate, paths)
        val dfWriteMap = process(dfMap)
        write(dfWriteMap, saveMode, incrDate)
      }
    }

  }

  def canProcess(incrDate: String, saveMode: String): Boolean

  def readDF(incrDate: String, prevDate: String, paths: String): HashMap[String, DataFrame]

  def process(dfMap: HashMap[String, DataFrame]): HashMap[String, DataFrame]

  def write(dfWrite: HashMap[String, DataFrame], saveMode: String, incrDate: String)

}
