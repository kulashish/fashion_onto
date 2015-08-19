package com.jabong.dap.quality.campaign

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.constants.campaign.{ CampaignMergedFields, CampaignCommon }
import com.jabong.dap.common.constants.status.OrderStatus
import com.jabong.dap.common.constants.variables.{ SalesOrderItemVariables, ProductVariables, SalesOrderVariables }
import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.common.udf.Udf
import org.apache.spark.sql.DataFrame

/**
 * Created by Kapil.Rajak on 12/8/15.
 * This Class is designed for backward test.
 *
 * Backward test means, getting a sample of campaign output, then for each entries in the sample,
 * we try to find the expected data in the campaign input Dataframes
 */
object CancelReTargetQuality extends BaseCampaignQuality {

  val campaignName = "CancelReTargetQuality"

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
   * @param cancelRetargetDF
   * @return
   */
  def validateOrderStatus(orderItemDF: DataFrame, orderDF: DataFrame, cancelRetargetDF: DataFrame): Boolean = {
    val cancelOrderItemDF = orderItemDF.filter(SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS + " in ("
      + OrderStatus.CANCELLED + "," + OrderStatus.CANCELLED_CC_ITEM + "," + OrderStatus.CANCEL_PAYMENT_ERROR
      + "," + OrderStatus.DECLINED + "," + OrderStatus.EXPORTABLE_CANCEL_CUST + "," + OrderStatus.EXPORTED_CANCEL_CUST + ")")

    val joinedOrder = orderDF.join(cancelOrderItemDF,
      orderDF(SalesOrderVariables.ID_SALES_ORDER).equalTo(cancelOrderItemDF(SalesOrderItemVariables.FK_SALES_ORDER)),
      "inner")
      .select(orderDF(SalesOrderVariables.FK_CUSTOMER),
        Udf.skuFromSimpleSku(cancelOrderItemDF(ProductVariables.SKU))).dropDuplicates()

    val cancelDF = cancelRetargetDF.select(CampaignMergedFields.CUSTOMER_ID, CampaignMergedFields.REF_SKU1).dropDuplicates()
    joinedOrder.intersect(cancelDF).count() == cancelDF.count()
  }

  /**
   *
   * @param date in 2015/08/01 format
   * @return
   */
  def getInputOutput(date: String = TimeUtils.YESTERDAY_FOLDER): (DataFrame, DataFrame, DataFrame) = {
    val orderItemDF = CampaignQualityEntry.orderItemData

    val orderDF = CampaignQualityEntry.last30DaysOrderData

    val cancelRetargetDF = CampaignInput.getCampaignData(CampaignCommon.CANCEL_RETARGET_CAMPAIGN, date)
    return (orderItemDF, orderDF, cancelRetargetDF)
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
    val (orderItemDF, orderDF, cancelRetargetDF) = getInputOutput(date)
    val sampleCancelRetargetDF = getSample(cancelRetargetDF, fraction)
    validate(orderItemDF, orderDF, sampleCancelRetargetDF)
  }
}
