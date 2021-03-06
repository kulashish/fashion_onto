package com.jabong.dap.quality.campaign

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.variables.{ SalesOrderItemVariables, SalesOrderVariables }
import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by Mubarak on 12/8/15.
 *
 */
object InvalidLowStockQuality extends BaseCampaignQuality {

  val campaignName = "InvalidLowStockQuality"

  def getName(): String = {
    campaignName
  }

  /**
   * Consists of all the validation components for Backward test
   * @param orderItemDF
   * @param invalidLow
   * @return
   */
  def validate(orderItemDF: DataFrame, invalidLow: DataFrame, itr: DataFrame): Boolean = {
    if ((orderItemDF == null) || invalidLow == null || itr == null)
      return invalidLow == null
    InvalidOrdersQuality.checkInvalidOrder(orderItemDF, invalidLow)
    // SkuSelectionQuality.validateLowStock(invalidFollowup, itr)
  }

  /**
   *
   * @param date in YYYY/MM/DD format
   * @return
   */
  def getInputOutput(date: String = TimeUtils.YESTERDAY_FOLDER): (DataFrame, DataFrame, DataFrame) = {
    val orderItemDF = CampaignQualityEntry.orderItem30DaysData

    val fullOrderData = CampaignQualityEntry.last30DaysOrderData.select(SalesOrderVariables.ID_SALES_ORDER, SalesOrderVariables.FK_CUSTOMER)

    val orderItemJoined = orderItemDF.join(fullOrderData, fullOrderData(SalesOrderVariables.ID_SALES_ORDER) === orderItemDF(SalesOrderItemVariables.FK_SALES_ORDER))

    val invalidLow = CampaignInput.getCampaignData(CampaignCommon.INVALID_LOWSTOCK_CAMPAIGN, DataSets.PUSH_CAMPAIGNS, date)

    val itr = CampaignInput.loadYesterdayItrSkuData()

    return (orderItemJoined, invalidLow, itr)
  }

  /**
   * Entry point
   * Backward test means, getting a sample of campaign output, then for each entries in the sample,
   * we try to find the expected data in the campaign input Dataframes
   * @param date
   * @param fraction
   * @return
   */
  def backwardTest(date: String, fraction: Double): Boolean = {
    val (orderItem, invalidLow, itr) = getInputOutput(date)
    val sampleInvalidLowDF = getSample(invalidLow, fraction)
    validate(orderItem, sampleInvalidLowDF, itr)
  }
}
