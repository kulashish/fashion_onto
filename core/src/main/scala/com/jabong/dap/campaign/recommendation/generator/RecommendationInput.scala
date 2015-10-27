package com.jabong.dap.campaign.recommendation.generator

import java.sql.Timestamp

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.variables.SalesOrderVariables
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import com.jabong.dap.common.Utils


/**
 * Created by rahul  on 27/8/15.
 */
object RecommendationInput {
  var orderItemFullData, lastdayItrData: DataFrame = null
  /**
   *
   */
  def loadCommonDataSets(date: String) {
    orderItemFullData = CampaignInput.loadFullOrderItemData(date)
    lastdayItrData = CampaignInput.loadYesterdayItrSkuData(date).cache()
  }

  def lastNdaysData(inputDataFrame: DataFrame, days: Int, date: String = TimeUtils.YESTERDAY_FOLDER, field: String = SalesOrderVariables.CREATED_AT): DataFrame = {
    val dateTimeMs = TimeUtils.changeDateFormat(date, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_TIME_FORMAT_MS)
    val ndaysOldTime = Timestamp.valueOf(TimeUtils.getDateAfterNDays(-days, TimeConstants.DATE_TIME_FORMAT_MS, dateTimeMs))
    val ndaysOldStartTime = TimeUtils.getStartTimestampMS(ndaysOldTime)

    //   val dateTimeMs = TimeUtils.changeDateFormat(date, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_TIME_FORMAT_MS)

    val dateTime = Timestamp.valueOf(dateTimeMs)
    val dateEndTime = TimeUtils.getEndTimestampMS(dateTime)
    val lastDaysData = Utils.getTimeBasedDataFrame(inputDataFrame, SalesOrderVariables.CREATED_AT, ndaysOldStartTime.toString, dateEndTime.toString)

    return lastDaysData
  }

}
