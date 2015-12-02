package com.jabong.dap.campaign.customerselection

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.variables._
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 7/9/15.
 */
class SalesCart extends CustomerSelector with Logging {

  override def customerSelection(last30DaySalesOrderData: DataFrame, last30DaySalesOrderItemData: DataFrame, salesCart30Days: DataFrame): DataFrame = {

    if (last30DaySalesOrderData == null || last30DaySalesOrderItemData == null || salesCart30Days == null) {
      logger.error("Data frame should not be null")
      return null

    }

    // FIXME: later how to handle sales cart data having sku instead of sku simple
    val dfSalesCart = salesCart30Days.filter(SalesCartVariables.STATUS + " = '" + SalesCartVariables.ACTIVE + "' or " + SalesCartVariables.STATUS + " = '" + SalesCartVariables.PURCHASED + "'")
      .select(
        SalesCartVariables.FK_CUSTOMER,
        SalesCartVariables.EMAIL,
        SalesCartVariables.SKU
      )
    CampaignUtils.debug(last30DaySalesOrderData, "last30DaySalesOrderData")

    CampaignUtils.debug(last30DaySalesOrderItemData, "last30DaySalesOrderItemData")

    CampaignUtils.debug(salesCart30Days, "salesCart30Days")

    CampaignUtils.debug(dfSalesCart, "dfSalesCart")

    val salesOrder = last30DaySalesOrderData.select(
      SalesOrderVariables.ID_SALES_ORDER,
      SalesOrderVariables.FK_CUSTOMER,
      SalesOrderVariables.CUSTOMER_EMAIL
    )
    val salesOrderItem = last30DaySalesOrderItemData.select(
      SalesOrderItemVariables.FK_SALES_ORDER,
      SalesOrderItemVariables.SKU
    )
    val joinedDf = salesOrder.join(
      salesOrderItem,
      salesOrder(SalesOrderVariables.ID_SALES_ORDER) === salesOrderItem(SalesOrderItemVariables.FK_SALES_ORDER),
      SQL.INNER
    ).select(
        salesOrder(SalesOrderVariables.FK_CUSTOMER),
        salesOrder(SalesOrderVariables.CUSTOMER_EMAIL) as CustomerVariables.EMAIL,
        salesOrderItem(SalesOrderItemVariables.SKU)
      )

    CampaignUtils.debug(joinedDf, "joinedDf")

    val dfUnion = joinedDf.unionAll(dfSalesCart).distinct

    CampaignUtils.debug(dfUnion, "dfUnion")

    return dfUnion.filter(!(dfUnion(SalesOrderVariables.FK_CUSTOMER).isNull && dfUnion(CustomerVariables.EMAIL).isNull))
  }

  override def customerSelection(salesCart30Day: DataFrame): DataFrame = ???
  override def customerSelection(inData: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, ndays: Int): DataFrame = ???

}
