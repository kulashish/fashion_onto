package com.jabong.dap.quality.campaign

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.{ CampaignMergedFields, CampaignCommon }
import com.jabong.dap.common.constants.variables.SalesOrderVariables
import com.jabong.dap.common.constants.variables.ACartVariables
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.storage.DataSets
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul on 14/8/15.
 */
object ACartPushCampaignQuality extends BaseCampaignQuality with Logging {

  val campaignName = "ACartPushCampaignQuality"

  def getName(): String = {
    campaignName
  }

  /**
   * Consists of all the validation components for Backward test
   * @param salesCartDF
   * @param sampleAcartFollowUp
   * @return
   */
  def validateAcartFollowup(salesCartDF: DataFrame, sampleAcartFollowUp: DataFrame): Boolean = {
    if ((salesCartDF == null) || sampleAcartFollowUp == null)
      return sampleAcartFollowUp == null
    return validateAbandonedCart(salesCartDF, sampleAcartFollowUp)
    //&& SkuSelectionQuality.validateFollowupStock(yesterdayItr,sampleAcartFollowUp)
  }

  def validateAcartLowStock(salesCartDF: DataFrame, sampleAcartLowStock: DataFrame): Boolean = {
    if ((salesCartDF == null) || sampleAcartLowStock == null)
      return sampleAcartLowStock == null
    return validateAbandonedCart(salesCartDF, sampleAcartLowStock)
    //&& SkuSelectionQuality.validateLowStock(yesterdayItr,sampleAcartLowStock)
  }

  def validateAcartIOD(salesCartDF: DataFrame, sampleAcartIOD: DataFrame): Boolean = {
    if ((salesCartDF == null) || sampleAcartIOD == null)
      return sampleAcartIOD == null
    return validateAbandonedCart(salesCartDF, sampleAcartIOD)
  }

  /**
   * One component checking if the data in sample output is present in orderItemDF with expected "Sales Order Item Status"
   * @param salesCartDF
   * @param sampleCampaignOutput
   * @return
   */
  def validateAbandonedCart(salesCartDF: DataFrame, sampleCampaignOutput: DataFrame): Boolean = {
    val updatedSalesCart = salesCartDF.select(salesCartDF(ACartVariables.FK_CUSTOMER),
      Udf.skuFromSimpleSku(salesCartDF(ACartVariables.SKU_SIMPLE)) as ACartVariables.SKU_SIMPLE,
      salesCartDF(ACartVariables.ACART_STATUS))

    val abandaonedCartCustomers = updatedSalesCart.join(sampleCampaignOutput, updatedSalesCart(SalesOrderVariables.FK_CUSTOMER) === sampleCampaignOutput(CampaignMergedFields.CUSTOMER_ID)
      && sampleCampaignOutput(CampaignMergedFields.REF_SKU1) === (updatedSalesCart(ACartVariables.SKU_SIMPLE)), SQL.INNER)
      .filter(ACartVariables.ACART_STATUS + "='active'").dropDuplicates()

    return (abandaonedCartCustomers.count == sampleCampaignOutput.count)
  }

  /**
   *
   * @param date in 2015/08/01 format
   * @return
   */
  def getInputOutput(date: String): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {
    val salesCart30Days = CampaignQualityEntry.salesCart30DaysData
    val salesCart3rdDay = CampaignQualityEntry.salesCart3rdDayData
    // val yesterDayItr30Days = CampaignInput.load30DayItrSkuSimpleData()
    // val yesterDayItrData = CampaignInput.loadYesterdayItrSimpleData()
    val acartFollowUp = CampaignInput.getCampaignData(CampaignCommon.ACART_FOLLOWUP_CAMPAIGN, date, DataSets.PUSH_CAMPAIGNS, CampaignCommon.VERY_LOW_PRIORITY)
    val acartIOD = CampaignInput.getCampaignData(CampaignCommon.ACART_IOD_CAMPAIGN, date, DataSets.PUSH_CAMPAIGNS, CampaignCommon.VERY_LOW_PRIORITY)
    val acartLowStock = CampaignInput.getCampaignData(CampaignCommon.ACART_LOWSTOCK_CAMPAIGN, date, DataSets.PUSH_CAMPAIGNS, CampaignCommon.VERY_LOW_PRIORITY)
    return (salesCart30Days, salesCart3rdDay, acartFollowUp, acartIOD, acartLowStock)
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
    val (salesCart30DaysDF, salesCart3rdDayDF, acartFollowUp, acartIOD, acartLowStock) = getInputOutput(date)
    val sampleAcartFollowUp = getSample(acartFollowUp, fraction)
    val sampleAcartIOD = getSample(acartIOD, fraction)
    val sampleAcartLowStock = getSample(acartLowStock, fraction)

    val acartFollowUpStatus = validateAcartFollowup(salesCart3rdDayDF, sampleAcartFollowUp)
    val acartLowStockStatus = validateAcartLowStock(salesCart30DaysDF, sampleAcartLowStock)
    val acartIODStatus = validateAcartIOD(salesCart30DaysDF, sampleAcartIOD)
    logger.info("acartFollowUpStatus:-" + acartFollowUpStatus + "acartIODStatus:-" + acartIODStatus + "acartLowStockStatus:-" + acartLowStockStatus)
    return acartFollowUpStatus && acartIODStatus && acartLowStockStatus
  }
}
