package com.jabong.dap.quality.campaign

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.status.OrderStatus
import com.jabong.dap.common.constants.variables.{ProductVariables, SalesOrderItemVariables, SalesOrderVariables}
import com.jabong.dap.common.time.TimeUtils
import org.apache.spark.sql.DataFrame

/**
 * Created by Mubarak on 12/8/15.
 *
 */
object InvalidFollowupQuality extends BaseCampaignQuality{

  /** Consists of all the validation components for Backward test
   * @param orderItemDF
   * @param invalidFollowup
   * @return
   */
  def validate(orderItemDF:DataFrame, invalidFollowup:DataFrame, itr: DataFrame): Boolean = {
    if((orderItemDF==null) || invalidFollowup == null || itr == null)
      return invalidFollowup == null
    InvalidOrdersQuality.checkInvalidOrder(orderItemDF, invalidFollowup)
   // SkuSelectionQuality.validateFollowupStock(invalidFollowup, itr)
  }



  /**
   *
   * @param date in YYYY/MM/DD format
   * @return
   */
  def getInputOutput(date:String=TimeUtils.YESTERDAY_FOLDER):(DataFrame, DataFrame, DataFrame)={
    val fullOrderItemData = CampaignInput.loadFullOrderItemData()
    val orderItemDF = CampaignInput.loadLastNdaysOrderItemData(1,fullOrderItemData)

    val fullOrderData = CampaignInput.loadFullOrderData(date)

    val orderItemJoined = orderItemDF.join(fullOrderData, fullOrderData(SalesOrderVariables.ID_SALES_ORDER) === orderItemDF(SalesOrderItemVariables.FK_SALES_ORDER))

    val invalidFollow = CampaignInput.getCampaignData(CampaignCommon.INVALID_FOLLOWUP_CAMPAIGN,date)

    val itr = CampaignInput.loadYesterdayItrSkuData()

    return(orderItemJoined, invalidFollow, itr)
  }

  /**Entry point
   * Backward test means, getting a sample of campaign output, then for each entries in the sample,
   * we try to find the expected data in the campaign input Dataframes
   * @param date
   * @param fraction
   * @return
   */
  def backwardTest(date:String, fraction:Double):Boolean = {
    val (orderItem, invalidFollow, itr) = getInputOutput(date)
    val sampleCancelRetargetDF = getSample(orderItem, fraction)
    validate(orderItem, invalidFollow, itr)
  }
}
