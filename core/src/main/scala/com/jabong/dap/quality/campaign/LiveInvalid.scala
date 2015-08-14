package com.jabong.dap.quality.campaign


import com.jabong.dap.common.constants.campaign.CampaignMergedFields
import com.jabong.dap.common.constants.status.OrderStatus
import com.jabong.dap.common.constants.variables.{SalesOrderItemVariables, SalesOrderVariables}
import com.jabong.dap.model.product.itr.variables.ITR
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by jabong on 12/8/15.
 */
object LiveInvalid {

  def checkInvalidOrder(sales: DataFrame, campaign: DataFrame): Boolean ={
    val joined = sales.join(campaign, sales(SalesOrderVariables.FK_CUSTOMER) === campaign(CampaignMergedFields.CUSTOMER_ID) &&
      sales(SalesOrderItemVariables.SKU) === campaign(CampaignMergedFields.REF_SKU1) )
                    .select(CampaignMergedFields.CUSTOMER_ID,
                            SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS,
                            SalesOrderItemVariables.UPDATED_AT)
    val ordered = joined.orderBy(SalesOrderItemVariables.UPDATED_AT)
    val map = ordered.map(e => (e(0).toString ->(e(1).toString)))
    val x = map.groupByKey()
    val y = x.map(e=> (e._1, checkOrders(e._2)))
    return false
  }

  def checkOrders(orders: Iterable[(String)]): Boolean ={
    if(orders.toList.length == 1){
      return true
    } else{
      orders.toList.foreach{
          e => if(Integer.parseInt(e) != OrderStatus.CANCEL_PAYMENT_ERROR || Integer.parseInt(e) != OrderStatus.CANCEL_PAYMENT_ERROR){
            return true
          } else{
            return false
          }
      }

    }
    return false
  }

  def checkLowStock(itr: DataFrame, campaign: DataFrame): Boolean={
    val joined = itr.join(campaign, itr(ITR.CONFIG_SKU) === campaign(CampaignMergedFields.REF_SKU1))
      .select(CampaignMergedFields.REF_SKU1,
        ITR.QUANTITY)
    joined.rdd.foreach(e => if(Integer.parseInt(e(1).toString)> 10){
      return false
    })
    return true
  }


  def checkStock(itr: DataFrame, campaign: DataFrame): Boolean={
    val joined = itr.join(campaign, itr(ITR.CONFIG_SKU) === campaign(CampaignMergedFields.REF_SKU1))
      .select(CampaignMergedFields.REF_SKU1,
        ITR.QUANTITY)
    joined.rdd.foreach(e => if(Integer.parseInt(e(1).toString)<= 10){
      return false
    })
    return true
  }

}
