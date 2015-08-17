package com.jabong.dap.quality.campaign


import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.campaign.{CampaignCommon, CampaignMergedFields}
import com.jabong.dap.common.constants.status.OrderStatus
import com.jabong.dap.common.constants.variables.{SalesOrderItemVariables, SalesOrderVariables}
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.product.itr.variables.ITR
import org.apache.spark.SparkConf
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
                            SalesOrderItemVariables.FK_SALES_ORDER_ITEM_STATUS,
                            SalesOrderItemVariables.UPDATED_AT).orderBy(desc(SalesOrderItemVariables.UPDATED_AT))

    val grouped = joined.groupBy(CampaignMergedFields.CUSTOMER_ID).agg(first(SalesOrderItemVariables.FK_SALES_ORDER_ITEM_STATUS) as "status")

    val fil = grouped.filter(grouped("status").!==(OrderStatus.CANCEL_PAYMENT_ERROR) || grouped("status").!==(OrderStatus.INVALID))
    /* val map = joined.map(e => (e(0).toString ->(e(1).toString,e(2).toString) ))
    val x = map.groupByKey()
    val y = x.map(e=> (e._1, checkOrders(e._2)))*/
    return fil.count() == 0
  }

/*  def checkOrders(orders: Iterable[(String, String)]): Boolean ={

      orders.toList.foreach{
          e => val (k, date) = e

            if(Integer.parseInt(k) != OrderStatus.CANCEL_PAYMENT_ERROR || Integer.parseInt(k) != OrderStatus.CANCEL_PAYMENT_ERROR){
            return true
          } else{
            return false
          }
      }

    return false
  }*/

  def checkLowStock(itr: DataFrame, campaign: DataFrame): Boolean={
    val joined = itr.join(campaign, itr(ITR.CONFIG_SKU) === campaign(CampaignMergedFields.REF_SKU1))
      .select(CampaignMergedFields.REF_SKU1,
        ITR.QUANTITY)
    joined.rdd.foreach(e => if(Integer.parseInt(e(1).toString)> CampaignCommon.LOW_STOCK_VALUE){
      return false
    })
    return true
  }


  def checkFollowupStock(itr: DataFrame, campaign: DataFrame): Boolean={
    val joined = itr.join(campaign, itr(ITR.CONFIG_SKU) === campaign(CampaignMergedFields.REF_SKU1))
      .select(CampaignMergedFields.REF_SKU1,
        ITR.QUANTITY)
    joined.rdd.foreach(e => if(Integer.parseInt(e(1).toString)<= CampaignCommon.FOLLOW_UP_STOCK_VALUE){
      return false
    })
    return true
  }


  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkExamples")
    Spark.init(conf)
    val invLow = Spark.getSqlContext().read.parquet("").sample(true,.10)
    val invFol = Spark.getSqlContext().read.parquet("").sample(true,.10)
    val itr = Spark.getSqlContext().read.format(DataSets.ORC).load("")
    val salesOrder = Spark.getSqlContext().read.parquet("")
    val salesOrderItem = Spark.getSqlContext().read.parquet("")
    val sales = salesOrder.join(salesOrderItem, salesOrder(SalesOrderVariables.ID_SALES_ORDER) === salesOrderItem(SalesOrderItemVariables.FK_SALES_ORDER))
                  .select(SalesOrderVariables.FK_CUSTOMER,
                          SalesOrderItemVariables.SKU,
                          SalesOrderItemVariables.FK_SALES_ORDER_ITEM_STATUS)
    val res = checkInvalidOrder(sales, invFol)



  }

}
