package com.jabong.dap.quality.campaign

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.{CampaignMergedFields, CampaignCommon}
import com.jabong.dap.common.constants.variables.SalesOrderVariables
import com.jabong.dap.common.constants.variables.ACartVariables
import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.common.udf.Udf
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul on 14/8/15.
 */
object ACartPushCampaignQuality extends BaseCampaignQuality with Logging {

  /** Consists of all the validation components for Backward test
    * @param salesCartDF
    * @param sampleAcartFollowUp
    * @return
    */
  def validateAcartFollowup(salesCartDF:DataFrame,sampleAcartFollowUp:DataFrame): Boolean = {
    if((salesCartDF==null) || sampleAcartFollowUp == null)
      return sampleAcartFollowUp == null
    return validateAbandonedCart(salesCartDF,sampleAcartFollowUp)
    //&& SkuSelectionQuality.validateFollowupStock(yesterdayItr,sampleAcartFollowUp)
  }

  def validateAcartLowStock(salesCartDF:DataFrame,sampleAcartLowStock:DataFrame): Boolean = {
    if((salesCartDF==null) || sampleAcartLowStock == null)
      return sampleAcartLowStock == null
    return validateAbandonedCart(salesCartDF,sampleAcartLowStock)
    //&& SkuSelectionQuality.validateLowStock(yesterdayItr,sampleAcartLowStock)
  }


  def validateAcartDaily(salesCartDF:DataFrame,sampleAcartDaily:DataFrame): Boolean = {
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
    val updatedSalesCart = salesCartDF.select(salesCartDF(ACartVariables.FK_CUSTOMER),
      Udf.skuFromSimpleSku(salesCartDF(ACartVariables.SKU_SIMPLE)) as ACartVariables.SKU_SIMPLE)

    val abandaonedCartCustomers = updatedSalesCart.join(sampleCampaignOutput,updatedSalesCart(SalesOrderVariables.FK_CUSTOMER)===sampleCampaignOutput(CampaignMergedFields.CUSTOMER_ID)
      && sampleCampaignOutput(CampaignMergedFields.REF_SKU1)=== (updatedSalesCart(ACartVariables.SKU_SIMPLE)) ,SQL.INNER)
      .filter(ACartVariables.ACART_STATUS+"='active'").dropDuplicates()

    return (abandaonedCartCustomers.count == sampleCampaignOutput.count)
  }

  /**
   *
   * @param date in 2015/08/01 format
   * @return
   */
  def getInputOutput(date:String):(DataFrame,DataFrame,DataFrame,DataFrame)={
    val salesCart30Days = CampaignInput.loadLast30daysAcartData()
   // val yesterDayItr30Days = CampaignInput.load30DayItrSkuSimpleData()
   // val yesterDayItrData = CampaignInput.loadYesterdayItrSimpleData()
    val acartFollowUp = CampaignInput.getCampaignData(CampaignCommon.ACART_DAILY_CAMPAIGN,date,CampaignCommon.VERY_LOW_PRIORITY)
    val acartDaily = CampaignInput.getCampaignData(CampaignCommon.ACART_DAILY_CAMPAIGN,date,CampaignCommon.VERY_LOW_PRIORITY)
    val acartLowStock = CampaignInput.getCampaignData(CampaignCommon.ACART_DAILY_CAMPAIGN,date,CampaignCommon.VERY_LOW_PRIORITY)
    return(salesCart30Days,acartFollowUp,acartDaily,acartLowStock)
  }

  /**Entry point
    * Backward test means, getting a sample of campaign output, then for each entries in the sample,
    * we try to find the expected data in the campaign input Dataframes
    * @param date
    * @param fraction
    * @return
    */
  def backwardTest(date:String, fraction:Double):Boolean = {
    val (salesCartDF,acartFollowUp,acartDaily,acartLowStock) = getInputOutput(date)
    val sampleAcart = getSample(acartFollowUp,fraction)
    val sampleAcartDaily = getSample(acartDaily,fraction)
    val sampleAcartLowStock = getSample(acartLowStock,fraction)

    val acartFollowUpStatus  = validateAcartFollowup(salesCartDF,sampleAcart)
    val acartDailyStatus = validateAcartLowStock(salesCartDF,sampleAcartDaily)
    val acartLowStockStatus = validateAcartDaily(salesCartDF,sampleAcartLowStock)
    logger.info("acartFollowUpStatus:-"+ acartFollowUpStatus +"acartDailyStatus:-"+acartDailyStatus+"acartLowStockStatus:-"+acartLowStockStatus)
    return acartFollowUpStatus && acartDailyStatus && acartLowStockStatus
  }
}
