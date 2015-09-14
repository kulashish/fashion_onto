package com.jabong.dap.campaign.skuselection

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.status.OrderStatus
import com.jabong.dap.common.constants.variables.{ CustomerVariables, ProductVariables, SalesOrderItemVariables }
import org.apache.spark.sql.DataFrame

/**
 * Target customers who have returned the items
 */
object ReturnReTarget {

  /**
   *  Override sku Filter method to filter skus based on different order return statuses
   * @param orderItemDataFrame
   * @return
   */
  def skuFilter(orderItemDataFrame: DataFrame): DataFrame = {
    if (orderItemDataFrame == null) {
      return null
    }

    val filteredSku = orderItemDataFrame.filter(SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS + " in (" + OrderStatus.RETURN
      + "," + OrderStatus.RETURN_PAYMENT_PENDING + ")")
      .orderBy(SalesOrderItemVariables.UNIT_PRICE)
      .select(
        orderItemDataFrame(CustomerVariables.FK_CUSTOMER),
        orderItemDataFrame(ProductVariables.SKU) as ProductVariables.SKU_SIMPLE,
        orderItemDataFrame(SalesOrderItemVariables.UNIT_PRICE) as ProductVariables.SPECIAL_PRICE)

    //val refSku = CampaignUtils.generateReferenceSku(filteredSku, CampaignCommon.NUMBER_REF_SKUS)

    return filteredSku
  }
}
