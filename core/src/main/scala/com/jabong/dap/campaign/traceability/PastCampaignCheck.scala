package com.jabong.dap.campaign.traceability

import com.jabong.dap.common.constants.campaign.CampaignMerge
import com.jabong.dap.common.constants.variables.{ SalesOrderItemVariables, SalesOrderVariables, ProductVariables, CustomerVariables }
import com.jabong.dap.common.time.{ Constants, TimeUtils }
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
    //FIXME: Add campaign type check in HashMap

    val filterDate = TimeUtils.getDateAfterNDays(-nDays, Constants.DATE_FORMAT)

    val mailTypeCustomers = pastCampaignData.filter(CampaignMerge.CAMPAIGN_MAIL_TYPE + " = " + campaignMailType + " and " + CampaignMerge.END_OF_DATE + " >= '" + filterDate + "'")
      .select(pastCampaignData(CampaignMerge.FK_CUSTOMER) as CustomerVariables.FK_CUSTOMER, pastCampaignData(CampaignMerge.REF_SKU1) as ProductVariables.SKU)
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

    val pastCampaignSendCustomers = getCampaignCustomers(pastCampaignData, campaignMailType, nDays).withColumnRenamed(CampaignMerge.FK_CUSTOMER, "pastCampaign_" + CampaignMerge.FK_CUSTOMER)
    val pastCampaignNotSendCustomers = customerSelected.join(pastCampaignSendCustomers, customerSelected(CustomerVariables.FK_CUSTOMER) === pastCampaignSendCustomers("pastCampaign_" + CampaignMerge.FK_CUSTOMER), "left_outer")
      .filter(
        "pastCampaign_" + CampaignMerge.FK_CUSTOMER + " is null"
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
   * @param customerSelected
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
      .withColumnRenamed(CampaignMerge.FK_CUSTOMER, "pastCampaign_" + CampaignMerge.FK_CUSTOMER)

    val customerSkuSelected = customerSkuSimpleSelected.
      withColumn(ProductVariables.SKU,Udf.skuFromSimpleSku(customerSkuSimpleSelected(ProductVariables.SKU_SIMPLE)))

    val pastCampaignNotSendCustomers = customerSkuSelected
      .join(pastCampaignSendCustomers, customerSkuSelected(CustomerVariables.FK_CUSTOMER) === pastCampaignSendCustomers("pastCampaign_" + CampaignMerge.FK_CUSTOMER)
    &&
      customerSkuSelected(ProductVariables.SKU_SIMPLE) === pastCampaignSendCustomers(CampaignMerge.REF_SKU1)
        , "left_outer")
      .filter(
        "pastCampaign_" + CampaignMerge.FK_CUSTOMER + " is null"
      )
      .select(
        customerSkuSelected(CustomerVariables.FK_CUSTOMER),
        customerSkuSelected(ProductVariables.SKU_SIMPLE)
      )

    return pastCampaignNotSendCustomers
  }
}
