package com.jabong.dap.quality.campaign

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.constants.campaign.{ CampaignMergedFields, CampaignCommon }
import com.jabong.dap.common.constants.status.OrderStatus
import com.jabong.dap.common.constants.variables.{ ProductVariables, SalesOrderVariables, SalesOrderItemVariables }
import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by Kapil.Rajak on 14/8/15.
 */
object ReturnReTargetQuality extends BaseCampaignQuality {

  val campaignName = "ReturnReTargetQuality"

  def getName(): String = {
    campaignName
  }
  /**
   * Consists of all the validation components for Backward test
   * @param orderItemDF
   * @param orderDF
   * @param sampleCancelRetargetDF
   * @return
   */
  def validate(orderItemDF: DataFrame, orderDF: DataFrame, sampleCancelRetargetDF: DataFrame): Boolean = {
    if ((orderDF == null && orderItemDF == null) || sampleCancelRetargetDF == null)
      return sampleCancelRetargetDF == null
    validateOrderStatus(orderItemDF, orderDF: DataFrame, sampleCancelRetargetDF)
  }

  /**
   * One component checking if the data in sample output is present in orderItemDF with expected "Sales Order Item Status"
   * @param orderItemDF
   * @param returnRetargetDF
   * @return
   */
  def validateOrderStatus(orderItemDF: DataFrame, orderDF: DataFrame, returnRetargetDF: DataFrame): Boolean = {
    val returnOrderItemDF = orderItemDF.filter(SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS + " in (" + OrderStatus.RETURN
      + "," + OrderStatus.RETURN_PAYMENT_PENDING + ")")

    val joinedOrder = orderDF.join(returnOrderItemDF,
      orderDF(SalesOrderVariables.ID_SALES_ORDER).equalTo(returnOrderItemDF(SalesOrderItemVariables.FK_SALES_ORDER)),
      "inner")
      .select(orderDF(SalesOrderVariables.FK_CUSTOMER),
        Udf.skuFromSimpleSku(returnOrderItemDF(ProductVariables.SKU))).dropDuplicates()

    val returnDF = returnRetargetDF.select(CampaignMergedFields.CUSTOMER_ID, CampaignMergedFields.REF_SKU1).dropDuplicates()
    joinedOrder.intersect(returnDF).count() == returnDF.count()
  }

  /**
   *
   * @param date in 2015/08/01 format
   * @return
   */
  def getInputOutput(date: String = TimeUtils.YESTERDAY_FOLDER): (DataFrame, DataFrame, DataFrame) = {
    val orderItemDF = CampaignQualityEntry.orderItemData

    val orderDF = CampaignQualityEntry.last30DaysOrderData

    val returnRetargetDF = CampaignInput.getCampaignData(CampaignCommon.RETURN_RETARGET_CAMPAIGN, DataSets.PUSH_CAMPAIGNS, date)
    return (orderItemDF, orderDF, returnRetargetDF)
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
    val (orderItemDF, orderDF, returnRetargetDF) = getInputOutput(date)
    val sampleCancelRetargetDF = getSample(returnRetargetDF, fraction)
    validate(orderItemDF, orderDF, sampleCancelRetargetDF)
  }
}
