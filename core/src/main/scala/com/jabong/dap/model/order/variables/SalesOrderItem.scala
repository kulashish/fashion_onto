package com.jabong.dap.model.order.variables

import com.jabong.dap.common.time.Constants
import com.jabong.dap.common.constants.variables.{ SalesOrderItemVariables, SalesOrderVariables }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.common.{ Spark }
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Created by jabong on 3/7/15.
 */
object SalesOrderItem {

  def processVariables(curr: String, prev: String) {
    val salesOrder = Spark.getSqlContext().read.parquet(DataSets.BOB_PATH + DataSets.SALES_ORDER + "/" + curr)
    val salesItem = Spark.getSqlContext().read.parquet(DataSets.BOB_PATH + DataSets.SALES_ORDER_ITEM + "/" + curr)
    val salesOrderNew = salesOrder.na.fill(Map(
      SalesOrderVariables.GW_AMOUNT -> 0.0))
    val salesOrderItem = salesOrderNew.join(salesItem, salesOrder(SalesOrderVariables.ID_SALES_ORDER) === salesItem(SalesOrderVariables.FK_SALES_ORDER))

  }

  def getSucessfulOrders(salesOrderItem: DataFrame, prevCount: DataFrame): DataFrame = {
    val sucessOrders = salesOrderItem.filter(salesOrderItem(SalesOrderItemVariables.FK_SALES_ORDER_ITEM_STATUS).contains(Constants.ITEM_SUCCESS_STATUS))
    val currCount = sucessOrders.groupBy(SalesOrderVariables.FK_CUSTOMER).agg(countDistinct(SalesOrderVariables.FK_SALES_ORDER) as SalesOrderItemVariables.ORDERS_COUNT)
    val currFull = currCount.unionAll(prevCount)
    val res = currFull.groupBy(SalesOrderVariables.FK_SALES_ORDER).agg(sum(SalesOrderItemVariables.ORDERS_COUNT) as SalesOrderItemVariables.ORDERS_COUNT)
    return res
  }

  def getRevenue(salesJoinedRDD: DataFrame): DataFrame = {
    val resultRDD = salesJoinedRDD.groupBy(SalesOrderVariables.FK_CUSTOMER).agg((first(SalesOrderVariables.SHIPPING_AMOUNT) + first(SalesOrderVariables.COD_CHARGE) + first(SalesOrderVariables.GW_AMOUNT) + sum(SalesOrderItemVariables.PAID_PRICE) + sum(SalesOrderItemVariables.GIFTCARD_CREDITS_VALUE) + sum(SalesOrderItemVariables.STORE_CREDITS_VALUE) + sum(SalesOrderItemVariables.PAYBACK_CREDITS_VALUE)) as SalesOrderItemVariables.REVENUE)
    val newRdd = resultRDD.rdd
    val schema = StructType(Array(StructField(SalesOrderVariables.FK_CUSTOMER, IntegerType, true), StructField(SalesOrderItemVariables.REVENUE, DecimalType.apply(16, 2), true)))
    val res = Spark.getSqlContext().createDataFrame(newRdd, schema)
    res.show(9)
    res
  }

  /**
   * for testing only
   * @param args
   */
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkExamples")
    Spark.init(conf)
    val df1 = Spark.getSqlContext().read.json("test/sales_order_item1.json").select(SalesOrderVariables.FK_CUSTOMER, SalesOrderItemVariables.FK_SALES_ORDER, SalesOrderItemVariables.FK_SALES_ORDER_ITEM_STATUS)
    val df2 = Spark.getSqlContext().read.json("test/sales_order_item2.json").select(SalesOrderVariables.FK_CUSTOMER, SalesOrderItemVariables.ORDERS_COUNT)
    df1.collect().foreach(println)
    val res = getSucessfulOrders(df1, df2)
    res.collect().foreach(println)
  }

}
