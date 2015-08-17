package com.jabong.dap.quality.campaign

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.variables.SalesOrderVariables
import com.jabong.dap.common.constants.variables.ACartVariables
import com.jabong.dap.common.time.TimeUtils
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul on 14/8/15.
 */
object ACartPushCampaignQuality {
  /** gives random selected rows from DataFrame
    * @param df
    * @param fraction 1 means give all data, .5 means give half of the data selecting in random
    * @return
    */
  def getSample(df:DataFrame, fraction:Double):DataFrame={
    df.sample(false,fraction)
  }

  /** Consists of all the validation components for Backward test
    * @param salesCartDF
    * @param yesterdayItr
    * @param sampleAcartFollowUp
    * @return
    */
  def validateAcartFollowup(salesCartDF:DataFrame,yesterdayItr:DataFrame,sampleAcartFollowUp:DataFrame): Boolean = {
    if((salesCartDF==null) || sampleAcartFollowUp == null)
      return sampleAcartFollowUp == null
    return validateAbandonedCart(salesCartDF,sampleAcartFollowUp) && SkuSelectionQuality.validateFollowupStock(yesterdayItr,sampleAcartFollowUp)
  }

  def validateAcartLowStock(salesCartDF:DataFrame,yesterdayItr:DataFrame,sampleAcartLowStock:DataFrame): Boolean = {
    if((salesCartDF==null) || sampleAcartLowStock == null)
      return sampleAcartLowStock == null
    return validateAbandonedCart(salesCartDF,sampleAcartLowStock) && SkuSelectionQuality.validateLowStock(yesterdayItr,sampleAcartLowStock)
  }


  def validateAcartDaily(salesCartDF:DataFrame,yesterdayItr:DataFrame,sampleAcartDaily:DataFrame): Boolean = {
    if((salesCartDF==null) || sampleAcartDaily == null)
      return sampleAcartDaily == null
    return validateAbandonedCart(salesCartDF,sampleAcartDaily)
  }

  /** One component checking if the data in sample output is present in orderItemDF with expected "Sales Order Item Status"
    * @param salesCartDF
    * @param sampleCampaignOutput
    * @return
    */
  def validateAbandonedCart(salesCartDF:DataFrame,sampleCampaignOutput:DataFrame): Boolean = {
    val notAbandaonedCartCustomers = salesCartDF.join(sampleCampaignOutput,salesCartDF(SalesOrderVariables.FK_CUSTOMER)===sampleCampaignOutput(SalesOrderVariables.FK_CUSTOMER),SQL.INNER)
      .filter(ACartVariables.ACART_STATUS+"!='active'")
    return (notAbandaonedCartCustomers.count == 0)
  }

  /**
   *
   * @param date in 2015/08/01 format
   * @return
   */
  def getInputOutput(date:String):(DataFrame,DataFrame,DataFrame)={
    val salesCart30Days = CampaignInput.loadLast30daysAcartData()
    val yesterDayItr30Days = CampaignInput.load30DayItrSkuSimpleData()
    val yesterDayItrData = CampaignInput.loadYesterdayItrSimpleData()
    val acartFollowUp = CampaignInput.getCampaignData(CampaignCommon.ACART_DAILY_CAMPAIGN,date,CampaignCommon.VERY_LOW_PRIORITY)
    val acartDaily = CampaignInput.getCampaignData(CampaignCommon.ACART_DAILY_CAMPAIGN,date,CampaignCommon.VERY_LOW_PRIORITY)
    val acartLowStock = CampaignInput.getCampaignData(CampaignCommon.ACART_DAILY_CAMPAIGN,date,CampaignCommon.VERY_LOW_PRIORITY)
    return(salesCart30Days,yesterDayItrData,acartFollowUp)
  }

  /**Entry point
    * Backward test means, getting a sample of campaign output, then for each entries in the sample,
    * we try to find the expected data in the campaign input Dataframes
    * @param date
    * @param fraction
    * @return
    */
  def backwardTest(date:String, fraction:Double):Boolean = {
    val (salesCartDF,orderDF,cancelRetargetDF) = getInputOutput(date)
    val sampleAcartFollowUpDF = getSample(cancelRetargetDF,fraction)
    validateAcartFollowup(salesCartDF,orderDF,sampleAcartFollowUpDF)
    validateAcartLowStock(salesCartDF,orderDF,sampleAcartFollowUpDF)
    validateAcartDaily(salesCartDF,orderDF,sampleAcartFollowUpDF)
  }
}
