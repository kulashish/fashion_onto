package com.jabong.dap.quality.campaign

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.variables.{SalesOrderItemVariables, SalesOrderVariables}
import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.quality.campaign.InvalidFollowupQuality._
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * Created by jabong on 18/8/15.
 */
object Surf3Quality extends BaseCampaignQuality with Logging{

  val campaignName = "Surf3Quality"

  def getName(): String = {
    campaignName
  }

  /** Consists of all the validation components for Backward test
    * @param surf3Data
    * @param surf3Campaign
    * @return
    */
  def validate(surf3Data: DataFrame, surf3Campaign: DataFrame, itr: DataFrame): Boolean = {
    if((surf3Data==null) || surf3Campaign == null || itr == null)
      return surf3Data == null
    checkCustomerSelection(surf3Data, surf3Campaign)
    //Not implementing sku selection since we getting sku from campaign not sku_simple
  }

  def checkCustomerSelection(surf3Data: DataFrame, surf3Campaign: DataFrame): Boolean={
    //TODO direct dating coming from processed variables
    return true
  }


  /**
   *
   * @param date in YYYY/MM/DD format
   * @return
   */
  def getInputOutput(date:String=TimeUtils.YESTERDAY_FOLDER):(DataFrame, DataFrame, DataFrame)={
    val fullOrderItemData = CampaignInput.loadFullOrderItemData()
    val surf3Data = CampaignInput.loadLastDaySurf3Data()
    val surf3Campaign = CampaignInput.getCampaignData(CampaignCommon.SURF3_CAMPAIGN,date)

    val yestItrSkuData = CampaignInput.loadYesterdayItrSkuData()
    return (surf3Data, surf3Campaign, yestItrSkuData)
  }

  /**Entry point
    * Backward test means, getting a sample of campaign output, then for each entries in the sample,
    * we try to find the expected data in the campaign input Dataframes
    * @param date
    * @param fraction
    * @return
    */
  def backwardTest(date:String, fraction:Double):Boolean = {
    val (surf3Data, surf3Campaign, itr) = getInputOutput(date)
    val surf3DataRetargetDF = getSample(surf3Campaign, fraction)
    validate(surf3Data, surf3DataRetargetDF, itr)
  }


}
