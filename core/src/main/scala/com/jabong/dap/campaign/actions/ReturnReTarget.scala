package com.jabong.dap.campaign.actions

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.status.OrderStatus
import com.jabong.dap.common.constants.variables.{CustomerVariables, ProductVariables, SalesOrderItemVariables}
import org.apache.spark.sql.DataFrame

/**
 * Target customers who have returned the items
 */
class ReturnReTarget extends Action {
  // val hiveContext = Spark.getHiveContext()
  override def execute(orderItemDataFrame: DataFrame): DataFrame = {

    if(orderItemDataFrame==null){
      return null
    }
    val filteredSku = skuFilter(orderItemDataFrame)
    val refSku= CampaignUtils.generateReferenceSku(filteredSku,1)
    refSku.foreach(println)
    return refSku

  }


  override def skuFilter(orderItemDataFrame: DataFrame): DataFrame ={
    if(orderItemDataFrame==null){
      return null
    }

    val filteredSku = orderItemDataFrame.filter(SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS+" in ("+OrderStatus.RETURN
      +","+OrderStatus.RETURN_PAYMENT_PENDING+")")
      .orderBy(SalesOrderItemVariables.UNIT_PRICE)
      .select(CustomerVariables.FK_CUSTOMER,ProductVariables.SKU,SalesOrderItemVariables.UNIT_PRICE)

    return filteredSku
  }



}
