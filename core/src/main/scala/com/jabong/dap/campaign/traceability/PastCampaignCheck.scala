package com.jabong.dap.campaign.traceability

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.campaign.manager.CampaignManager
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.CampaignMergedFields
import com.jabong.dap.common.constants.variables.{PageVisitVariables, CustomerVariables, ContactListMobileVars, ProductVariables}
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

  val past30DayMobileCampaignMergedData: DataFrame = CampaignInput.load30DayCampaignMergedData(DataSets.PUSH_CAMPAIGNS).cache()
  val past30DayEmailCampaignMergedData: DataFrame = CampaignInput.load30DayCampaignMergedData(DataSets.EMAIL_CAMPAIGNS).cache()

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
      return emailCampaignRefSkuCheck(pastCampaignData, customerSkuSimpleSelected, campaignMailType, nDays)
    } else if (campaignType.equals(DataSets.PUSH_CAMPAIGNS)) {
      pastCampaignData = past30DayMobileCampaignMergedData
      return pushCampaignRefSkuCheck(pastCampaignData, customerSkuSimpleSelected, campaignMailType, nDays)
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

    var customerNotNullSkuSimpleSelected: DataFrame = customerSkuSimpleSelected
    var pastCampaignNullSendCustomers: DataFrame = null

    val pastCampaignSendCustomers = getCampaignCustomers(pastCampaignData, campaignMailType, nDays).withColumnRenamed(CustomerVariables.FK_CUSTOMER, "pastCampaign_" + CustomerVariables.FK_CUSTOMER)
    val surfStatus: Boolean = customerSkuSimpleSelected.schema.toList.contains(PageVisitVariables.BROWSER_ID)

    if(surfStatus){
      val customerNullSkuSimpleSelected = customerSkuSimpleSelected.filter(CustomerVariables.FK_CUSTOMER +" is null")
      customerNotNullSkuSimpleSelected = customerSkuSimpleSelected.filter(CustomerVariables.FK_CUSTOMER +" is not null and "+CustomerVariables.FK_CUSTOMER +" != 0")

       pastCampaignNullSendCustomers =  customerNullSkuSimpleSelected
        .join(pastCampaignSendCustomers, customerNullSkuSimpleSelected(PageVisitVariables.BROWSER_ID) === pastCampaignSendCustomers(CampaignMergedFields.DEVICE_ID)
        &&
        customerNullSkuSimpleSelected(ProductVariables.SKU) === pastCampaignSendCustomers(CampaignMergedFields.LIVE_REF_SKU1), SQL.LEFT_OUTER)
        .filter(
          CampaignMergedFields.DEVICE_ID + " is null"
        ).select(
          customerNullSkuSimpleSelected("*")
        )
    }

    val customerSkuSelected = customerNotNullSkuSimpleSelected.
      withColumn("temp_" + ProductVariables.SKU, Udf.skuFromSimpleSku(customerNotNullSkuSimpleSelected(ProductVariables.SKU_SIMPLE)))

    val pastCampaignNotNullSendCustomers = customerSkuSelected
      .join(pastCampaignSendCustomers, customerSkuSelected(CustomerVariables.FK_CUSTOMER) === pastCampaignSendCustomers("pastCampaign_" + CustomerVariables.FK_CUSTOMER)
        &&
        customerSkuSelected("temp_" + ProductVariables.SKU) === pastCampaignSendCustomers(CampaignMergedFields.LIVE_REF_SKU1), SQL.LEFT_OUTER)
      .filter(
        "pastCampaign_" + CustomerVariables.FK_CUSTOMER + " is null"
      )
      .select(
        customerSkuSelected("*")
      )
    var pastCampaignNotSendCustomers: DataFrame = pastCampaignNotNullSendCustomers

    if(surfStatus) pastCampaignNotSendCustomers = pastCampaignNotSendCustomers.unionAll(pastCampaignNullSendCustomers)
    
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

    val customerSkuSelected = customerSkuSimpleSelected.
      withColumn(ProductVariables.SKU, Udf.skuFromSimpleSku(customerSkuSimpleSelected(ProductVariables.SKU_SIMPLE)))

    val pastCampaignNotSendEmail = customerSkuSelected
      .join(pastCampaignSendCustomers,
        (customerSkuSelected(CustomerVariables.EMAIL) === pastCampaignSendCustomers(ContactListMobileVars.EMAIL))
          &&
          ((customerSkuSelected(ProductVariables.SKU) === pastCampaignSendCustomers(CampaignMergedFields.LIVE_REF_SKU1))
            ||
            (customerSkuSelected(ProductVariables.SKU) === pastCampaignSendCustomers(CampaignMergedFields.LIVE_REF_SKU + "2"))
            ||
            (customerSkuSelected(ProductVariables.SKU) === pastCampaignSendCustomers(CampaignMergedFields.LIVE_REF_SKU + "3"))), SQL.LEFT_OUTER)
      .filter(
        CampaignMergedFields.EMAIL + " is null "
      )
      .select(
        customerSkuSimpleSelected("*")
      )

    return pastCampaignNotSendEmail
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
