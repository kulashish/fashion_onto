package com.jabong.dap.model.campaignFeeds

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.data.acq.common.ParamInfo
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.HashMap

/**
 * Created by pooja on 30/10/15.
 */
abstract class CampaignFeedsModel {

  def start(params: ParamInfo): Unit = {
    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val saveMode = params.saveMode
    val paths = OptionUtils.getOptValue(params.path)
    val prevDate = OptionUtils.getOptValue(params.fullDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER, incrDate))

    if (canProcess(incrDate, saveMode)) {
      val dfMap = readDF(incrDate, prevDate, paths)
      val dfWriteMap = process(dfMap)
      write(dfWriteMap, saveMode, incrDate)
    }
  }

  def canProcess(incrDate: String, saveMode: String): Boolean

  def readDF(incrDate: String, prevDate: String, paths: String): HashMap[String, DataFrame]

  def process(dfMap: HashMap[String, DataFrame]): HashMap[String, DataFrame]

  def write(dfWrite: HashMap[String, DataFrame], saveMode: String, incrDate: String)

}
