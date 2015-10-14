package com.jabong.dap.campaign.traceability

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.campaign.manager.CampaignManager
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.CampaignMergedFields
import com.jabong.dap.common.constants.variables.{ ContactListMobileVars, CustomerVariables, ProductVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.storage.DataSets
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * To check whether we send the campaign to the user earlier
 */
object PastCampaignCheck extends Logging {

  val past30DayMobileCampaignMergedData: DataFrame = CampaignInput.load30DayCampaignMergedData(DataSets.PUSH_CAMPAIGNS)
  val past30DayEmailCampaignMergedData: DataFrame = CampaignInput.load30DayCampaignMergedData(DataSets.EMAIL_CAMPAIGNS)

  /**
   *
   * @param pastCampaignData
   * @param campaignMailType
   * @param nDays
   * @return
   */
  def getCampaignCustomers(pastCampaignData: DataFrame, campaignMailType: Int, nDays: Int, campaignType: String = DataSets.PUSH_CAMPAIGNS): DataFrame = {
    if (pastCampaignData == null || campaignMailType == 0 || nDays < 0) {
      logger.error("Any of the argument is null")
      return null
    }
    if (!CampaignManager.mailTypePriorityMap.contains(campaignMailType)) {
      logger.error("Invalid CampaignType")
      return null
    }

    val filterDate = TimeUtils.getDateAfterNDays(-nDays, TimeConstants.DATE_FORMAT)

    var mailTypeCustomers: DataFrame = null
    if (campaignType.equals(DataSets.PUSH_CAMPAIGNS)) {
      mailTypeCustomers = pastCampaignData.filter(CampaignMergedFields.LIVE_MAIL_TYPE + " = " + campaignMailType + " and " + CampaignMergedFields.END_OF_DATE + " >= '" + filterDate + "'")
        .select(pastCampaignData(CampaignMergedFields.CUSTOMER_ID) as CustomerVariables.FK_CUSTOMER,
          pastCampaignData(CampaignMergedFields.LIVE_REF_SKU1))
    } else if (campaignType.equals(DataSets.EMAIL_CAMPAIGNS)) {
      mailTypeCustomers = pastCampaignData.filter(CampaignMergedFields.LIVE_MAIL_TYPE + " = " + campaignMailType + " and " + CampaignMergedFields.LAST_UPDATED_DATE + " >= '" + filterDate + "'")
    }

    logger.info("Filtering campaign customer based on mail type " + campaignMailType + " and date >= " + filterDate)

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
    val pastCampaignNotSendCustomers = customerSelected.join(pastCampaignSendCustomers, customerSelected(CustomerVariables.FK_CUSTOMER) === pastCampaignSendCustomers("pastCampaign_" + CustomerVariables.FK_CUSTOMER), SQL.LEFT_OUTER)
      .filter(
        "pastCampaign_" + CustomerVariables.FK_CUSTOMER + " is null"
      )
      .select(
        customerSelected("*")
      )

    return pastCampaignNotSendCustomers
  }

  def campaignCommonRefSkuCheck(campaignType: String, customerSkuSimpleSelected: DataFrame, campaignMailType: Int, nDays: Int): DataFrame = {
    var pastCampaignData: DataFrame = null

    if (campaignType.equals(DataSets.EMAIL_CAMPAIGNS)) {
      pastCampaignData = past30DayEmailCampaignMergedData
      emailCampaignRefSkuCheck(pastCampaignData, customerSkuSimpleSelected, campaignMailType, nDays)
    } else if (campaignType.equals(DataSets.PUSH_CAMPAIGNS)) {
      pastCampaignData = past30DayMobileCampaignMergedData
      pushCampaignRefSkuCheck(pastCampaignData, customerSkuSimpleSelected, campaignMailType, nDays)
    }
    logger.info("Invalid campaign Type :- returning the same  customer selected data ")
    return customerSkuSimpleSelected
  }

  /**
   *  To check whether the campaign has been sent to customer for the same ref sku in last nDays
   * @param pastCampaignData
   * @param customerSkuSimpleSelected
   * @param campaignMailType
   * @param nDays
   * @return
   */
  def pushCampaignRefSkuCheck(pastCampaignData: DataFrame, customerSkuSimpleSelected: DataFrame, campaignMailType: Int, nDays: Int): DataFrame = {
    if (pastCampaignData == null || customerSkuSimpleSelected == null || campaignMailType == 0 || nDays < 0) {
      logger.error("Any of the argument is null")
      return customerSkuSimpleSelected
    }

    val pastCampaignSendCustomers = getCampaignCustomers(pastCampaignData, campaignMailType, nDays).withColumnRenamed(CustomerVariables.FK_CUSTOMER, "pastCampaign_" + CustomerVariables.FK_CUSTOMER)

    val customerSkuSelected = customerSkuSimpleSelected.
      withColumn("temp_" + ProductVariables.SKU, Udf.skuFromSimpleSku(customerSkuSimpleSelected(ProductVariables.SKU_SIMPLE)))

    val pastCampaignNotSendCustomers = customerSkuSelected
      .join(pastCampaignSendCustomers, customerSkuSelected(CustomerVariables.FK_CUSTOMER) === pastCampaignSendCustomers("pastCampaign_" + CustomerVariables.FK_CUSTOMER)
        &&
        customerSkuSelected("temp_" + ProductVariables.SKU) === pastCampaignSendCustomers(CampaignMergedFields.LIVE_REF_SKU1), SQL.LEFT_OUTER)
      .filter(
        "pastCampaign_" + CustomerVariables.FK_CUSTOMER + " is null"
      )
      .select(
        customerSkuSelected("*")
      )

    return pastCampaignNotSendCustomers
  }

  /**
   *  To check whether the email campaign has been sent to customer for the same ref sku in last nDays
   * @param pastCampaignData
   * @param customerSkuSimpleSelected
   * @param campaignMailType
   * @param nDays
   * @return
   */
  def emailCampaignRefSkuCheck(pastCampaignData: DataFrame, customerSkuSimpleSelected: DataFrame, campaignMailType: Int, nDays: Int): DataFrame = {
    if (pastCampaignData == null || customerSkuSimpleSelected == null || campaignMailType == 0 || nDays < 0) {
      logger.error("Any of the argument is null")
      return customerSkuSimpleSelected
    }

    val pastCampaignSendCustomers = getCampaignCustomers(pastCampaignData, campaignMailType, nDays, DataSets.EMAIL_CAMPAIGNS)

    var dfCustomerSkuSimpleSelected = customerSkuSimpleSelected

    if (!customerSkuSimpleSelected.schema.fieldNames.toList.contains(CustomerVariables.EMAIL)) {
      dfCustomerSkuSimpleSelected = customerSkuSimpleSelected.withColumn(CustomerVariables.EMAIL, lit(null))
    }

    val customerSkuSelected = dfCustomerSkuSimpleSelected.
      withColumn("temp_" + ProductVariables.SKU, Udf.skuFromSimpleSku(dfCustomerSkuSimpleSelected(ProductVariables.SKU_SIMPLE)))

    val pastCampaignNotSendCustomers = customerSkuSelected
      .join(pastCampaignSendCustomers,
        (Udf.isEquals(customerSkuSelected(CustomerVariables.EMAIL), pastCampaignSendCustomers(ContactListMobileVars.EMAIL)) || Udf.isEquals(customerSkuSelected(CustomerVariables.FK_CUSTOMER), pastCampaignSendCustomers(CampaignMergedFields.CUSTOMER_ID)))
          &&
          (customerSkuSelected("temp_" + ProductVariables.SKU) === pastCampaignSendCustomers(CampaignMergedFields.LIVE_REF_SKU1))
          ||
          (customerSkuSelected("temp_" + ProductVariables.SKU) === pastCampaignSendCustomers(CampaignMergedFields.LIVE_REF_SKU + "2"))
          ||
          (customerSkuSelected("temp_" + ProductVariables.SKU) === pastCampaignSendCustomers(CampaignMergedFields.LIVE_REF_SKU + "3")), SQL.LEFT_OUTER)
      .filter(
        CampaignMergedFields.CUSTOMER_ID + " is null "
      )
      .select(
        customerSkuSelected("*")
      )

    return pastCampaignNotSendCustomers
  }

  def getLastNDaysData(n: Int): DataFrame = {
    var data: DataFrame = null
    for (i <- 1 to n) {
      val date = TimeUtils.getDateAfterNDays(-i, "yyyy/MM/dd")
      var df = CampaignInput.loadCampaignOutput(date)
      data = data.unionAll(df)
    }
    data
  }
}
