package com.jabong.dap.model.order.variables

import com.jabong.dap.common.time.Constants
import com.jabong.dap.common.constants.variables.{SalesOrderItemVariables, SalesOrderVariables}
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.common.{Spark}
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._



/**
 * Created by mubarak on 3/7/15.
 */
object SalesOrderItem {

  def processVariables(curr: String, prev:String ) {
    val salesOrder = Spark.getSqlContext().read.parquet(DataSets.BOB_PATH + DataSets.SALES_ORDER + "/" + curr)
    val salesItem = Spark.getSqlContext().read.parquet(DataSets.BOB_PATH + DataSets.SALES_ORDER_ITEM + "/" + curr)
    val salesOrderNew = salesOrder.na.fill(Map(
      SalesOrderVariables.GW_AMOUNT -> 0.0
    ))
    val salesJoinedDF = salesOrderNew.join(salesItem, salesOrderNew(SalesOrderVariables.ID_SALES_ORDER) === salesItem(SalesOrderVariables.FK_SALES_ORDER))
    val appOrders = salesJoinedDF.filter(SalesOrderItemVariables.FILTER_APP)
    val webOrders = salesJoinedDF.filter(SalesOrderItemVariables.FILTER_WEB)
    val mWebOrders = salesJoinedDF.filter(SalesOrderItemVariables.FILTER_MWEB)
    val app = getRevenueOrders(appOrders,"_app")
    val web = getRevenueOrders(webOrders,"_web")
    val mWeb = getRevenueOrders(mWebOrders,"_mweb")
    val joinedData = joinDataFrames(app, web, mWeb)
    val prevFull = Spark.getSqlContext().read.parquet(DataSets.BOB_PATH + DataSets.SALES_ORDER + "/full/" + prev)
    val mergedData = merge(joinedData, prevFull)
    mergedData.write.parquet(DataSets.BOB_PATH + DataSets.SALES_ORDER + "/full/" + curr)
  }

  def joinDataFrames(app: DataFrame, web: DataFrame, mWeb: DataFrame): DataFrame ={
    val bcapp = Spark.getContext().broadcast(app).value
    val bcweb = Spark.getContext().broadcast(web).value
    val bcmweb = Spark.getContext().broadcast(mWeb).value

    /*val appJoined = bcweb.join(bcapp, bcapp.value(SalesOrderVariables.FK_CUSTOMER) === bcweb(SalesOrderVariables.FK_CUSTOMER), "outer").
      select(coalesce(bcapp.value(SalesOrderVariables.FK_CUSTOMER),
        bcweb(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
        bcweb(SalesOrderItemVariables.ORDERS_COUNT_WEB),
        bcweb(SalesOrderItemVariables.REVENUE_WEB),
        bcapp(SalesOrderItemVariables.ORDERS_COUNT_APP),
        bcapp(SalesOrderItemVariables.REVENUE_APP))
    appJoined.show(5)
    val joinedData = appJoined.join(bcmweb, bcmweb(SalesOrderVariables.FK_CUSTOMER) === appJoined(SalesOrderVariables.FK_CUSTOMER), "outer").
      select(coalesce(bcmweb.value(SalesOrderVariables.FK_CUSTOMER),
      appJoined(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
      appJoined(SalesOrderItemVariables.ORDERS_COUNT_WEB),
      appJoined(SalesOrderItemVariables.REVENUE_WEB),
      appJoined(SalesOrderItemVariables.ORDERS_COUNT_APP),
      appJoined(SalesOrderItemVariables.REVENUE_APP),
      bcmweb(SalesOrderItemVariables.ORDERS_COUNT_MWEB),
      bcmweb(SalesOrderItemVariables.REVENUE_MWEB)).na.fill(Map(
      SalesOrderItemVariables.ORDERS_COUNT_APP -> 0,
      SalesOrderItemVariables.ORDERS_COUNT_WEB -> 0,
      SalesOrderItemVariables.ORDERS_COUNT_MWEB -> 0,
      SalesOrderItemVariables.REVENUE_APP -> 0.0,
      SalesOrderItemVariables.REVENUE_MWEB -> 0.0,
      SalesOrderItemVariables.REVENUE_WEB -> 0.0
    ))
    joinedData.show(5)
    val res = joinedData.withColumn(SalesOrderItemVariables.REVENUE,
      joinedData(SalesOrderItemVariables.REVENUE_APP) + joinedData(SalesOrderItemVariables.REVENUE_WEB) + joinedData(SalesOrderItemVariables.REVENUE_MWEB) ).withColumn(
      SalesOrderItemVariables.ORDERS_COUNT,
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_APP) + joinedData(SalesOrderItemVariables.ORDERS_COUNT_WEB) + joinedData(SalesOrderItemVariables.ORDERS_COUNT_MWEB))
    */return web
  }

  def merge(inc :DataFrame, full :DataFrame): DataFrame ={
    val bcInc = Spark.getContext().broadcast(inc)
    val joinedData = full.join(bcInc.value, bcInc.value(SalesOrderVariables.FK_CUSTOMER) === full(SalesOrderVariables.FK_CUSTOMER), "outer" )
    val res = joinedData.select(
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_LIFE)+ joinedData(SalesOrderItemVariables.ORDERS_COUNT) as SalesOrderItemVariables.ORDERS_COUNT_LIFE,
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_APP_LIFE)+ joinedData(SalesOrderItemVariables.ORDERS_COUNT_APP) as SalesOrderItemVariables.ORDERS_COUNT_APP_LIFE,
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_WEB_LIFE)+ joinedData(SalesOrderItemVariables.ORDERS_COUNT_WEB) as SalesOrderItemVariables.ORDERS_COUNT_WEB_LIFE,
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_MWEB_LIFE)+ joinedData(SalesOrderItemVariables.ORDERS_COUNT_MWEB) as SalesOrderItemVariables.ORDERS_COUNT_MWEB_LIFE,
      joinedData(SalesOrderItemVariables.REVENUE_LIFE)+ joinedData(SalesOrderItemVariables.REVENUE) as SalesOrderItemVariables.REVENUE_LIFE,
      joinedData(SalesOrderItemVariables.REVENUE_APP_LIFE)+ joinedData(SalesOrderItemVariables.REVENUE_APP) as SalesOrderItemVariables.REVENUE_APP_LIFE,
      joinedData(SalesOrderItemVariables.REVENUE_WEB_LIFE)+ joinedData(SalesOrderItemVariables.REVENUE_WEB) as SalesOrderItemVariables.REVENUE_WEB_LIFE,
      joinedData(SalesOrderItemVariables.REVENUE_MWEB_LIFE)+ joinedData(SalesOrderItemVariables.REVENUE_MWEB) as SalesOrderItemVariables.REVENUE_MWEB_LIFE
    )
    return res
  }

  /**
   *  Difficult because, should link items to sales_order
   * @param salesOrderItem
   * @param prevCount
   * @return
   */
  def getSucessfulOrders(salesOrderItem: DataFrame, prevCount: DataFrame): DataFrame={
    val sucessOrders = salesOrderItem.filter(salesOrderItem(SalesOrderItemVariables.FK_SALES_ORDER_ITEM_STATUS).contains(Constants.ITEM_SUCCESS_STATUS))
    val currCount = sucessOrders.groupBy(SalesOrderVariables.FK_CUSTOMER).agg(countDistinct(SalesOrderVariables.FK_SALES_ORDER) as SalesOrderItemVariables.ORDERS_COUNT)
    val currFull = currCount.unionAll(prevCount)
    val res = currFull.groupBy(SalesOrderVariables.FK_SALES_ORDER).agg(sum(SalesOrderItemVariables.ORDERS_COUNT) as SalesOrderItemVariables.ORDERS_COUNT)
    return res
  }

  def getRevenueOrders(salesOrderItem: DataFrame, domain: String): DataFrame ={
    val resultRDD = salesOrderItem.groupBy(SalesOrderVariables.FK_CUSTOMER).agg((first(SalesOrderVariables.SHIPPING_AMOUNT) + first(SalesOrderVariables.COD_CHARGE) + first(SalesOrderVariables.GW_AMOUNT) + sum(SalesOrderItemVariables.PAID_PRICE) + sum(SalesOrderItemVariables.GIFTCARD_CREDITS_VALUE) +  sum(SalesOrderItemVariables.STORE_CREDITS_VALUE) + sum(SalesOrderItemVariables.PAYBACK_CREDITS_VALUE)) as SalesOrderItemVariables.REVENUE, countDistinct(SalesOrderItemVariables.FK_SALES_ORDER) as SalesOrderVariables.ORDERS_COUNT)
    val newRdd = resultRDD.rdd
    val schema = StructType(Array(StructField(SalesOrderVariables.FK_CUSTOMER, LongType, true), StructField(SalesOrderItemVariables.REVENUE+domain, DecimalType.apply(16,2), true), StructField(SalesOrderVariables.ORDERS_COUNT+domain, IntegerType , true)))
    val res = Spark.getSqlContext().createDataFrame(newRdd, schema)
    res.printSchema()
    res.show(5)
    res
  }


  /**
   * for testing only
   * @param args
   */
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkExamples")
    Spark.init(conf)

    val soi = StructType(Array(
      StructField(SalesOrderVariables.FK_CUSTOMER, LongType, true),
      StructField(SalesOrderItemVariables.ID_SALES_ORDER_ITEM, IntegerType, true),
      StructField(SalesOrderVariables.COD_CHARGE,  DecimalType(10, 2), true),
      StructField(SalesOrderVariables.GW_AMOUNT,  DecimalType(10, 2), true),
      StructField(SalesOrderVariables.SHIPPING_AMOUNT,  DecimalType(10, 2), true),
      StructField(SalesOrderItemVariables.FK_SALES_ORDER, IntegerType, true),
      StructField(SalesOrderItemVariables.GIFTCARD_CREDITS_VALUE, DecimalType(10, 2), true),
      StructField(SalesOrderItemVariables.PAYBACK_CREDITS_VALUE, DecimalType(10, 2), true),
      StructField(SalesOrderItemVariables.PAID_PRICE, DecimalType(10, 2), true),
      StructField(SalesOrderItemVariables.STORE_CREDITS_VALUE, DecimalType(10, 2), true)
    ))
//    val df1 = Spark.getSqlContext().read.json("test/sales_order_item1.json").select(SalesOrderVariables.FK_CUSTOMER,SalesOrderItemVariables.FK_SALES_ORDER, SalesOrderItemVariables.FK_SALES_ORDER_ITEM_STATUS)
    val df2 = Spark.getSqlContext().read.schema(soi).format("json").load("test/sales_order_item_web.json")
    val web = getRevenueOrders(df2,"_web")
    val df3 = Spark.getSqlContext().read.schema(soi).format("json").load("test/sales_order_item_app.json")
    val app = getRevenueOrders(df3,"_app")
    val df4 = Spark.getSqlContext().read.schema(soi).format("json").load("test/sales_order_item_mweb.json")
    val mweb = getRevenueOrders(df4,"_mweb")
    val res = web.join(app, web("fk_customer") === app("fk_customer"),"outer")
    //val res5 = joinDataFrames(app,web,mweb)
    res.collect().foreach(println)
  }


}
