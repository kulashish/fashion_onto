package com.jabong.dap.quality.campaign

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.constants.campaign.{CampaignMergedFields, CampaignCommon}
import com.jabong.dap.common.constants.variables.{CustomerPageVisitVariables, SalesOrderItemVariables, SalesOrderVariables}
import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.common.udf.Udf
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by jabong on 18/8/15.
 */
object Surf6Quality  extends BaseCampaignQuality with Logging{

  val campaignName = "Surf6Quality"

  def getName(): String = {
    campaignName
  }

  /** Consists of all the validation components for Backward test
    * @param surf6Data
    * @param surf6Campaign
    * @return
    */
  def validate(surf6Data: DataFrame, surf6Campaign: DataFrame): Boolean = {
    if((surf6Data==null) || surf6Campaign == null)
      return surf6Data == null
    checkCustomerSelection(surf6Data, surf6Campaign)
   // checkSkuSelection(surf6Campaign, itr)

  }

  def checkCustomerSelection(surf6Data: DataFrame, surf6Campaign: DataFrame): Boolean={

    val dfDistinctSku = surf6Data.select(
      col(CustomerPageVisitVariables.USER_ID),
      col(CustomerPageVisitVariables.ACTUAL_VISIT_ID),
      col(CustomerPageVisitVariables.BROWER_ID),
      col(CustomerPageVisitVariables.DOMAIN),
      Udf.distinctSku(col(CustomerPageVisitVariables.SKU_LIST)) as CustomerPageVisitVariables.SKU_LIST
    )

    val dfCountSku = dfDistinctSku.select(
      col(CustomerPageVisitVariables.USER_ID),
      col(CustomerPageVisitVariables.ACTUAL_VISIT_ID),
      col(CustomerPageVisitVariables.BROWER_ID),
      col(CustomerPageVisitVariables.DOMAIN),
      col(CustomerPageVisitVariables.SKU_LIST),
      Udf.countSku(dfDistinctSku(CustomerPageVisitVariables.SKU_LIST)) as CustomerPageVisitVariables.COUNT_SKU
    )
    val joined = dfCountSku.join(surf6Campaign, dfCountSku(CustomerPageVisitVariables.BROWER_ID) === surf6Campaign(CampaignMergedFields.DEVICE_ID))
                            .select(CampaignMergedFields.DEVICE_ID,
        CustomerPageVisitVariables.COUNT_SKU)

    var temp: Int = 0
    joined.rdd.foreach(e => (if(Integer.parseInt(e(1).toString) < 5){
      temp = temp + 1
    }))
    println("Count < 5 = "+temp)
    return (temp ==0 && joined.count() == surf6Campaign.count())
  }

  def checkSkuSelection(surf6Campaign: DataFrame, itr: DataFrame): Boolean={
    //TODo same for all the surf campaigns should be common for all
    // Not implementing now since we getting sku from campaign not sku_simple
   return true
  }


  /**
   *
   * @param date in YYYY/MM/DD format
   * @return
   */
  def getInputOutput(date:String=TimeUtils.YESTERDAY_FOLDER):(DataFrame, DataFrame)={

    val surf6Data = CampaignQualityEntry.yestSessionData

    val surf6Campaign = CampaignInput.getCampaignData(CampaignCommon.SURF6_CAMPAIGN,date)

    return (surf6Data, surf6Campaign)
  }

  /**Entry point
    * Backward test means, getting a sample of campaign output, then for each entries in the sample,
    * we try to find the expected data in the campaign input Dataframes
    * @param date
    * @param fraction
    * @return
    */
  def backwardTest(date:String, fraction:Double):Boolean = {
    val (surf6Data, surf6Campaign, itr) = getInputOutput(date)
    val surf6CampaignRetargetDF = getSample(surf6Campaign, fraction)
    validate(surf6Data, surf6CampaignRetargetDF)
  }


}
