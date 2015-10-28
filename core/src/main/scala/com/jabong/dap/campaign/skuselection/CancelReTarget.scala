package com.jabong.dap.campaign.skuselection

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.status.OrderStatus
import com.jabong.dap.common.constants.variables.{ CustomerVariables, ProductVariables, SalesOrderItemVariables }
import org.apache.spark.sql.{ Row, DataFrame }
import org.apache.spark.sql.functions._

/**
 * Cancel Re-target class
 */
object CancelReTarget {

  /**
   * Override sku Filter method to filter skus based on different order cancel statuses
   * @param inDataFrame
   * @return
   */
  def skuFilter(inDataFrame: DataFrame): DataFrame = {
    if (inDataFrame == null) {
      return null
    }

    val filteredSku = inDataFrame.filter(SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS + "=" + OrderStatus.CANCELLED
      + " or " + SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS + "=" + OrderStatus.CANCELLED_CC
      + " or " + SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS + "=" + OrderStatus.CANCEL_PAYMENT_ERROR
      + " or " + SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS + "=" + OrderStatus.DECLINED
      + " or " + SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS + "=" + OrderStatus.EXPORTABLE_CANCEL_CUST
      + " or " + SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS + "=" + OrderStatus.EXPORTED_CANCEL_CUST)
      .orderBy(SalesOrderItemVariables.UNIT_PRICE)
      .select(inDataFrame(CustomerVariables.FK_CUSTOMER),
        inDataFrame(ProductVariables.SKU) as ProductVariables.SKU_SIMPLE,
        inDataFrame(SalesOrderItemVariables.UNIT_PRICE))

    // val refSku = CampaignUtils.generateReferenceSku(filteredSku, CampaignCommon.NUMBER_REF_SKUS)

    return filteredSku
  }

}
