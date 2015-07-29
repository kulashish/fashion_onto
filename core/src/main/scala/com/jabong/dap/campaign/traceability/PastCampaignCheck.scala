package com.jabong.dap.campaign.traceability

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.campaign.manager.CampaignManager
import com.jabong.dap.common.constants.campaign.CampaignMergedFields
import com.jabong.dap.common.constants.variables.{ CustomerVariables, ProductVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.Udf
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * To check whether we send the campaign to the user earlier
 */
class PastCampaignCheck extends Logging {

  /**
   *
   * @param pastCampaignData
   * @param campaignMailType
   * @param nDays
   * @return
   */
  def getCampaignCustomers(pastCampaignData: DataFrame, campaignMailType: Int, nDays: Int): DataFrame = {
    if (pastCampaignData == null || campaignMailType == 0 || nDays < 0) {
      logger.error("Any of the argument is null")
      return null
    }
    if (!CampaignManager.mailTypePriorityMap.contains(campaignMailType)) {
      logger.error("Invalid CampaignType")
      return null
    }

    val filterDate = TimeUtils.getDateAfterNDays(-nDays, TimeConstants.DATE_FORMAT)

    val mailTypeCustomers = pastCampaignData.filter(CampaignMergedFields.CAMPAIGN_MAIL_TYPE + " = " + campaignMailType + " and " + CampaignMergedFields.END_OF_DATE + " >= '" + filterDate + "'")
      .select(pastCampaignData(CampaignMergedFields.CUSTOMER_ID) as CustomerVariables.FK_CUSTOMER,
        pastCampaignData(CampaignMergedFields.REF_SKU1) as ProductVariables.SKU,
        pastCampaignData(CampaignMergedFields.DEVICE_ID))


    logger.info("Filtering campaign customer based on mail type" + campaignMailType + " and date >= " + filterDate)

    return mailTypeCustomers
  }

  /**
   *  To check whether the campaign has been sent to customer in last nDays
   * @param pastCampaignData
   * @param customerSelected
   * @param campaignMailType
   * @param nDays
   * @return
   */
  def campaignCheck(pastCampaignData: DataFrame, customerSelected: DataFrame, campaignMailType: Int, nDays: Int): DataFrame = {
    if (pastCampaignData == null || customerSelected == null || campaignMailType == 0 || nDays < 0) {
      logger.error("Any of the argument is null")
      return null
    }

    val pastCampaignSendCustomers = getCampaignCustomers(pastCampaignData, campaignMailType, nDays).withColumnRenamed(CustomerVariables.FK_CUSTOMER, "pastCampaign_" + CustomerVariables.FK_CUSTOMER)
    val pastCampaignNotSendCustomers = customerSelected.join(pastCampaignSendCustomers, customerSelected(CustomerVariables.FK_CUSTOMER) === pastCampaignSendCustomers("pastCampaign_" + CustomerVariables.FK_CUSTOMER), "left_outer")
      .filter(
        "pastCampaign_" + CustomerVariables.FK_CUSTOMER + " is null"
      )
      .select(
        customerSelected(CustomerVariables.FK_CUSTOMER),
        customerSelected(ProductVariables.SKU_SIMPLE)
      )

    return pastCampaignNotSendCustomers
  }

  /**
   *  To check whether the campaign has been sent to customer for the same ref sku in last nDays
   * @param pastCampaignData
   * @param customerSkuSimpleSelected
   * @param campaignMailType
   * @param nDays
   * @return
   */
  def campaignRefSkuCheck(pastCampaignData: DataFrame, customerSkuSimpleSelected: DataFrame, campaignMailType: Int, nDays: Int): DataFrame = {
    if (pastCampaignData == null || customerSkuSimpleSelected == null || campaignMailType == 0 || nDays < 0) {
      logger.error("Any of the argument is null")
      return null
    }

    val pastCampaignSendCustomers = getCampaignCustomers(pastCampaignData, campaignMailType, nDays)
      .withColumnRenamed(CampaignMergedFields.CUSTOMER_ID, "pastCampaign_" + CampaignMergedFields.CUSTOMER_ID)

    val customerSkuSelected = customerSkuSimpleSelected.
      withColumn(ProductVariables.SKU, Udf.skuFromSimpleSku(customerSkuSimpleSelected(ProductVariables.SKU_SIMPLE)))

    val pastCampaignNotSendCustomers = customerSkuSelected
      .join(pastCampaignSendCustomers, customerSkuSelected(CustomerVariables.FK_CUSTOMER) === pastCampaignSendCustomers("pastCampaign_" + CampaignMergedFields
        .CUSTOMER_ID)
        &&
        customerSkuSelected(ProductVariables.SKU_SIMPLE) === pastCampaignSendCustomers(CampaignMergedFields.REF_SKU1), "left_outer")
      .filter(
        "pastCampaign_" + CampaignMergedFields.CUSTOMER_ID + " is null"
      )
      .select(
        customerSkuSelected(CustomerVariables.FK_CUSTOMER),
        customerSkuSelected(ProductVariables.SKU_SIMPLE)
      )

    return pastCampaignNotSendCustomers
  }

  def getLastNDaysData(n: Int): DataFrame={
    var data: DataFrame = null
    for(i <-1 to n){
      val date = TimeUtils.getDateAfterNDays(-i, "yyyy/MM/dd")
      var df = CampaignInput.loadCampaignOutput(date)
      data = data.unionAll(df)
    }
    data
  }
}
