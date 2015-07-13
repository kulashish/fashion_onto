package com.jabong.dap.campaign.skuselection

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.status.OrderStatus
import com.jabong.dap.common.constants.variables.{ CustomerVariables, ProductVariables, SalesOrderItemVariables }
import org.apache.spark.sql.DataFrame

/**
 * Target customers who have returned the items
 */
class ReturnReTarget extends SkuSelector {
  override def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame, campaignName: String): DataFrame = ???

  //  // val hiveContext = Spark.getHiveContext()
  //  def execute(orderItemDataFrame: DataFrame): DataFrame = {
  //
  //    if (orderItemDataFrame == null) {
  //      return null
  //    }
  //    val filteredSku = skuFilter(orderItemDataFrame)
  //    refSku.foreach(println)
  //    return refSku
  //
  //  }

  /**
   *  Override sku Filter method to filter skus based on different order return statuses
   * @param orderItemDataFrame
   * @return
   */
  override def skuFilter(orderItemDataFrame: DataFrame): DataFrame = {
    if (orderItemDataFrame == null) {
      return null
    }

    val filteredSku = orderItemDataFrame.filter(SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS + " in (" + OrderStatus.RETURN
      + "," + OrderStatus.RETURN_PAYMENT_PENDING + ")")
      .orderBy(SalesOrderItemVariables.UNIT_PRICE)
      .select(CustomerVariables.FK_CUSTOMER, ProductVariables.SKU, SalesOrderItemVariables.UNIT_PRICE)

    val refSku = CampaignUtils.generateReferenceSkus(filteredSku, CampaignCommon.PUSH_REF_SKUS)

    return refSku
  }

}
