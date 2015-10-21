package com.jabong.dap.model.order.variables

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.variables.{ ProductVariables, SalesOrderItemVariables, SalesOrderVariables }
import com.jabong.dap.common.udf.Udf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Created by mubarak on 3/7/15.
 */
object SalesOrderItem {

  /**
   * Creates order_count(app,web,mweb) and Revenue(app,web,mweb)
   * @param salesOrder sales_order table data
   * @param salesItem sales_order_table data
   * @return Dataframe with the latest values for orders_count and ravenue for each customer
   */

  def processVariables(salesOrder: DataFrame, salesItem: DataFrame, prevFull: DataFrame, before7: DataFrame, before30: DataFrame, before90: DataFrame): (DataFrame, DataFrame) = {
    val salesOrderNew = salesOrder.na.fill(Map(
      SalesOrderVariables.GW_AMOUNT -> 0.0
    ))
    val salesJoinedDF = salesOrderNew.join(salesItem, salesOrderNew(SalesOrderVariables.ID_SALES_ORDER) === salesItem(SalesOrderVariables.FK_SALES_ORDER)).select(
      SalesOrderVariables.FK_CUSTOMER,
      SalesOrderVariables.COD_CHARGE,
      SalesOrderVariables.GW_AMOUNT,
      SalesOrderVariables.SHIPPING_AMOUNT,
      SalesOrderVariables.ID_SALES_ORDER,
      SalesOrderItemVariables.GIFTCARD_CREDITS_VALUE,
      SalesOrderItemVariables.PAYBACK_CREDITS_VALUE,
      SalesOrderItemVariables.PAID_PRICE,
      SalesOrderItemVariables.STORE_CREDITS_VALUE,
      SalesOrderVariables.DOMAIN
    )
    salesJoinedDF.printSchema()
    val appOrders = salesJoinedDF.filter(SalesOrderItemVariables.FILTER_APP)
    val webOrders = salesJoinedDF.filter(SalesOrderItemVariables.FILTER_WEB)
    val mWebOrders = salesJoinedDF.filter(SalesOrderItemVariables.FILTER_MWEB)
    val app = getRevenueOrders(appOrders, "_app")
    val web = getRevenueOrders(webOrders, "_web")
    val mWeb = getRevenueOrders(mWebOrders, "_mweb")
    val joinedData = joinDataFrames(app, web, mWeb)
    val mergedData = merge(joinedData, prevFull)
    val res7 = getRevenueDays(before7, mergedData, 7, 30, 90)
    val res30 = getRevenueDays(before30, res7, 30, 7, 90)
    val res = getRevenueDays(before30, res30, 90, 7, 30)
    (joinedData, res)
  }

  /**
   *
   * @param app DataFrame for app data
   * @param web DataFrame for web data
   * @param mWeb DataFrame for mobile_web data
   * @return Combined dataframe for all the above dataframes
   */
  def joinDataFrames(app: DataFrame, web: DataFrame, mWeb: DataFrame): DataFrame = {
    val bcapp = Spark.getContext().broadcast(app).value
    val bcweb = Spark.getContext().broadcast(web).value
    val bcmweb = Spark.getContext().broadcast(mWeb).value
    val appJoined = bcweb.join(bcapp, bcapp(SalesOrderVariables.FK_CUSTOMER) === bcweb(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER).
      select(
        coalesce(
          bcapp(SalesOrderVariables.FK_CUSTOMER),
          bcweb(SalesOrderVariables.FK_CUSTOMER)
        ) as SalesOrderVariables.FK_CUSTOMER,
        bcweb(SalesOrderItemVariables.ORDERS_COUNT_WEB),
        bcweb(SalesOrderItemVariables.REVENUE_WEB),
        bcapp(SalesOrderItemVariables.ORDERS_COUNT_APP),
        bcapp(SalesOrderItemVariables.REVENUE_APP)
      )
    appJoined.printSchema()
    appJoined.show(5)
    val joinedData = appJoined.join(bcmweb, bcmweb(SalesOrderVariables.FK_CUSTOMER) === appJoined(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER).
      select(
        coalesce(
          bcmweb(SalesOrderVariables.FK_CUSTOMER),
          appJoined(SalesOrderVariables.FK_CUSTOMER)
        ) as SalesOrderVariables.FK_CUSTOMER,
        appJoined(SalesOrderItemVariables.ORDERS_COUNT_WEB),
        appJoined(SalesOrderItemVariables.REVENUE_WEB),
        appJoined(SalesOrderItemVariables.ORDERS_COUNT_APP),
        appJoined(SalesOrderItemVariables.REVENUE_APP),
        bcmweb(SalesOrderItemVariables.ORDERS_COUNT_MWEB),
        bcmweb(SalesOrderItemVariables.REVENUE_MWEB)
      ).na.fill(Map(
          SalesOrderItemVariables.ORDERS_COUNT_APP -> 0,
          SalesOrderItemVariables.ORDERS_COUNT_WEB -> 0,
          SalesOrderItemVariables.ORDERS_COUNT_MWEB -> 0,
          SalesOrderItemVariables.REVENUE_APP -> 0.0,
          SalesOrderItemVariables.REVENUE_MWEB -> 0.0,
          SalesOrderItemVariables.REVENUE_WEB -> 0.0
        ))
    val res = joinedData.withColumn(
      SalesOrderItemVariables.REVENUE,
      joinedData(SalesOrderItemVariables.REVENUE_APP) + joinedData(SalesOrderItemVariables.REVENUE_WEB) + joinedData(SalesOrderItemVariables.REVENUE_MWEB)
    ).withColumn(
        SalesOrderItemVariables.ORDERS_COUNT,
        joinedData(SalesOrderItemVariables.ORDERS_COUNT_APP) + joinedData(SalesOrderItemVariables.ORDERS_COUNT_WEB) + joinedData(SalesOrderItemVariables.ORDERS_COUNT_MWEB)
      )
    res.printSchema()
    res.show(5)
    res
  }

  /**
   * Merges the incremental dataframe with the previous full dataframe
   * @param inc
   * @param full
   * @return merged full dataframe
   */
  def merge(inc: DataFrame, full: DataFrame): DataFrame = {
    val bcInc = Spark.getContext().broadcast(inc)
    val joinedData = full.join(bcInc.value, bcInc.value(SalesOrderVariables.FK_CUSTOMER) === full(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
    val res = joinedData.select(
      coalesce(
        full(SalesOrderVariables.FK_CUSTOMER),
        bcInc.value(SalesOrderVariables.FK_CUSTOMER)
      ) as SalesOrderVariables.FK_CUSTOMER,
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_LIFE) + joinedData(SalesOrderItemVariables.ORDERS_COUNT) as SalesOrderItemVariables.ORDERS_COUNT_LIFE,
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_APP_LIFE) + joinedData(SalesOrderItemVariables.ORDERS_COUNT_APP) as SalesOrderItemVariables.ORDERS_COUNT_APP_LIFE,
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_WEB_LIFE) + joinedData(SalesOrderItemVariables.ORDERS_COUNT_WEB) as SalesOrderItemVariables.ORDERS_COUNT_WEB_LIFE,
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_MWEB_LIFE) + joinedData(SalesOrderItemVariables.ORDERS_COUNT_MWEB) as SalesOrderItemVariables.ORDERS_COUNT_MWEB_LIFE,
      joinedData(SalesOrderItemVariables.REVENUE_LIFE) + joinedData(SalesOrderItemVariables.REVENUE) as SalesOrderItemVariables.REVENUE_LIFE,
      joinedData(SalesOrderItemVariables.REVENUE_APP_LIFE) + joinedData(SalesOrderItemVariables.REVENUE_APP) as SalesOrderItemVariables.REVENUE_APP_LIFE,
      joinedData(SalesOrderItemVariables.REVENUE_WEB_LIFE) + joinedData(SalesOrderItemVariables.REVENUE_WEB) as SalesOrderItemVariables.REVENUE_WEB_LIFE,
      joinedData(SalesOrderItemVariables.REVENUE_MWEB_LIFE) + joinedData(SalesOrderItemVariables.REVENUE_MWEB) as SalesOrderItemVariables.REVENUE_MWEB_LIFE,
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_7) + joinedData(SalesOrderItemVariables.ORDERS_COUNT) as SalesOrderItemVariables.ORDERS_COUNT_7,
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_APP_7) + joinedData(SalesOrderItemVariables.ORDERS_COUNT_APP) as SalesOrderItemVariables.ORDERS_COUNT_APP_7,
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_WEB_7) + joinedData(SalesOrderItemVariables.ORDERS_COUNT_WEB) as SalesOrderItemVariables.ORDERS_COUNT_WEB_7,
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_MWEB_7) + joinedData(SalesOrderItemVariables.ORDERS_COUNT_MWEB) as SalesOrderItemVariables.ORDERS_COUNT_MWEB_7,
      joinedData(SalesOrderItemVariables.REVENUE_7) + joinedData(SalesOrderItemVariables.REVENUE) as SalesOrderItemVariables.REVENUE_7,
      joinedData(SalesOrderItemVariables.REVENUE_APP_7) + joinedData(SalesOrderItemVariables.REVENUE_APP) as SalesOrderItemVariables.REVENUE_APP_7,
      joinedData(SalesOrderItemVariables.REVENUE_WEB_7) + joinedData(SalesOrderItemVariables.REVENUE_WEB) as SalesOrderItemVariables.REVENUE_WEB_7,
      joinedData(SalesOrderItemVariables.REVENUE_MWEB_7) + joinedData(SalesOrderItemVariables.REVENUE_MWEB) as SalesOrderItemVariables.REVENUE_MWEB_7,
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_30) + joinedData(SalesOrderItemVariables.ORDERS_COUNT) as SalesOrderItemVariables.ORDERS_COUNT_30,
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_APP_30) + joinedData(SalesOrderItemVariables.ORDERS_COUNT_APP) as SalesOrderItemVariables.ORDERS_COUNT_APP_30,
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_WEB_30) + joinedData(SalesOrderItemVariables.ORDERS_COUNT_WEB) as SalesOrderItemVariables.ORDERS_COUNT_WEB_30,
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_MWEB_30) + joinedData(SalesOrderItemVariables.ORDERS_COUNT_MWEB) as SalesOrderItemVariables.ORDERS_COUNT_MWEB_30,
      joinedData(SalesOrderItemVariables.REVENUE_30) + joinedData(SalesOrderItemVariables.REVENUE) as SalesOrderItemVariables.REVENUE_30,
      joinedData(SalesOrderItemVariables.REVENUE_APP_30) + joinedData(SalesOrderItemVariables.REVENUE_APP) as SalesOrderItemVariables.REVENUE_APP_30,
      joinedData(SalesOrderItemVariables.REVENUE_WEB_30) + joinedData(SalesOrderItemVariables.REVENUE_WEB) as SalesOrderItemVariables.REVENUE_WEB_30,
      joinedData(SalesOrderItemVariables.REVENUE_MWEB_30) + joinedData(SalesOrderItemVariables.REVENUE_MWEB) as SalesOrderItemVariables.REVENUE_MWEB_30,
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_90) + joinedData(SalesOrderItemVariables.ORDERS_COUNT) as SalesOrderItemVariables.ORDERS_COUNT_90,
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_APP_90) + joinedData(SalesOrderItemVariables.ORDERS_COUNT_APP) as SalesOrderItemVariables.ORDERS_COUNT_APP_90,
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_WEB_90) + joinedData(SalesOrderItemVariables.ORDERS_COUNT_WEB) as SalesOrderItemVariables.ORDERS_COUNT_WEB_90,
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_MWEB_90) + joinedData(SalesOrderItemVariables.ORDERS_COUNT_MWEB) as SalesOrderItemVariables.ORDERS_COUNT_MWEB_90,
      joinedData(SalesOrderItemVariables.REVENUE_90) + joinedData(SalesOrderItemVariables.REVENUE) as SalesOrderItemVariables.REVENUE_90,
      joinedData(SalesOrderItemVariables.REVENUE_APP_90) + joinedData(SalesOrderItemVariables.REVENUE_APP) as SalesOrderItemVariables.REVENUE_APP_90,
      joinedData(SalesOrderItemVariables.REVENUE_WEB_90) + joinedData(SalesOrderItemVariables.REVENUE_WEB) as SalesOrderItemVariables.REVENUE_WEB_90,
      joinedData(SalesOrderItemVariables.REVENUE_MWEB_90) + joinedData(SalesOrderItemVariables.REVENUE_MWEB) as SalesOrderItemVariables.REVENUE_MWEB_90
    )
    res.printSchema()
    res.show(5)
    res
  }

  /**
   *
   * @param salesOrderItemIncr
   * @return
   */
  def getSuccessfullOrdersBrand(salesOrderItemIncr: DataFrame, salesOrderFull: DataFrame,
                                dfSuccessOrdersCalcPrevFull: DataFrame, dfFavBrandCalcPrevFull: DataFrame,
                                yestItr: DataFrame): (DataFrame, DataFrame, DataFrame, DataFrame) = {
    val soiIncrSelected = salesOrderItemIncr
      .select(
        salesOrderItemIncr(SalesOrderItemVariables.FK_SALES_ORDER),
        salesOrderItemIncr(SalesOrderItemVariables.SKU),
        Udf.successOrder(salesOrderItemIncr(SalesOrderItemVariables.FK_SALES_ORDER_ITEM_STATUS)) as "STATUS"
      ).cache()

    val salesOrderJoined = soiIncrSelected.join(salesOrderFull, soiIncrSelected(SalesOrderItemVariables.FK_SALES_ORDER) === salesOrderFull(SalesOrderVariables.ID_SALES_ORDER))
    val (ordersCount, successOrdersUnion) = getSuccessfullOrders(salesOrderJoined, dfSuccessOrdersCalcPrevFull)

    val (favBrandIncr, favBrandUnion) = getMostPreferredBrand(salesOrderJoined, dfFavBrandCalcPrevFull, yestItr)

    (ordersCount.cache(), successOrdersUnion, favBrandIncr.cache(), favBrandUnion)
  }

  def getMostPreferredBrand(salesOrderJoined: DataFrame, dfFavBrandCalcPrevFull: DataFrame, yestItr: DataFrame): (DataFrame, DataFrame) = {
    var salesOrderJoinedIncr = salesOrderJoined

    if (null != dfFavBrandCalcPrevFull) {
      salesOrderJoinedIncr = salesOrderJoined.select(SalesOrderVariables.FK_CUSTOMER, SalesOrderVariables.ID_SALES_ORDER, SalesOrderItemVariables.SKU)
        .except(dfFavBrandCalcPrevFull.select(SalesOrderVariables.FK_CUSTOMER, SalesOrderVariables.ID_SALES_ORDER, SalesOrderItemVariables.SKU))
    }

    val mostPrefBrandJoinedIncr = salesOrderJoinedIncr.join(yestItr, salesOrderJoined(SalesOrderItemVariables.SKU) === yestItr(ProductVariables.SKU_SIMPLE))
      .select(
        col(SalesOrderVariables.FK_CUSTOMER),
        col(SalesOrderVariables.ID_SALES_ORDER),
        col(SalesOrderItemVariables.SKU),
        col(ProductVariables.BRAND),
        Udf.bigDecimal2Double(col(ProductVariables.SPECIAL_PRICE)) as ProductVariables.SPECIAL_PRICE
      )

    var mostPrefBrandIncr = mostPrefBrandJoinedIncr

    var mostPrefBrandUnion = mostPrefBrandJoinedIncr

    if (null != dfFavBrandCalcPrevFull) {
      mostPrefBrandUnion = dfFavBrandCalcPrevFull.unionAll(mostPrefBrandIncr)
      mostPrefBrandIncr = mostPrefBrandJoinedIncr.join(mostPrefBrandUnion,
        mostPrefBrandJoinedIncr(SalesOrderVariables.FK_CUSTOMER) === mostPrefBrandUnion(SalesOrderVariables.FK_CUSTOMER),
        SQL.LEFT_OUTER)
        .select(
          mostPrefBrandUnion(SalesOrderVariables.FK_CUSTOMER),
          mostPrefBrandUnion(SalesOrderVariables.ID_SALES_ORDER),
          mostPrefBrandUnion(SalesOrderItemVariables.SKU),
          mostPrefBrandUnion(ProductVariables.BRAND),
          mostPrefBrandUnion(ProductVariables.SPECIAL_PRICE))
    }

    val SUM_SPECIAL_PRICE = "sum_special_price"
    val COUNT_BRAND = "count_brand"

    val favBrandIncr = mostPrefBrandIncr.groupBy(SalesOrderVariables.FK_CUSTOMER, ProductVariables.BRAND)
      .agg(count(ProductVariables.BRAND) as COUNT_BRAND, sum(ProductVariables.SPECIAL_PRICE) as SUM_SPECIAL_PRICE)
      .sort(COUNT_BRAND, SUM_SPECIAL_PRICE)
      .groupBy(SalesOrderVariables.FK_CUSTOMER)
      .agg(last(ProductVariables.BRAND) as SalesOrderItemVariables.FAV_BRAND)

    (favBrandIncr, mostPrefBrandUnion)
  }

  /**
   *
   * @param salesOrderJoined
   * @return
   */
  def getSuccessfullOrders(salesOrderJoined: DataFrame, dfSalesOrderItemCalcPrevFull: DataFrame): (DataFrame, DataFrame) = {
    val successOrdersJoined = salesOrderJoined
      .select(
        col(SalesOrderVariables.FK_CUSTOMER),
        col(SalesOrderItemVariables.FK_SALES_ORDER) as SalesOrderVariables.ID_SALES_ORDER,
        col("STATUS"))
      .filter("STATUS = 1")
      .dropDuplicates()
    var newOrders = successOrdersJoined
    var successOrdersUnion = successOrdersJoined
    if (null != dfSalesOrderItemCalcPrevFull) {
      newOrders = successOrdersJoined.except(dfSalesOrderItemCalcPrevFull)
      successOrdersUnion = dfSalesOrderItemCalcPrevFull.unionAll(newOrders)
    }
    val ordersCount = newOrders.groupBy(SalesOrderVariables.FK_CUSTOMER)
      .agg(count("STATUS") as SalesOrderItemVariables.ORDERS_COUNT_SUCCESSFUL)

    (ordersCount, successOrdersUnion)
  }

  /**
   * Calculates revenue, orders_count per customer for the input dataframe
   * @param salesOrderItem
   * @param domain
   * @return dataframe with the revnue and orders count
   */
  def getRevenueOrders(salesOrderItem: DataFrame, domain: String): DataFrame = {
    val resultDF = salesOrderItem.groupBy(SalesOrderVariables.FK_CUSTOMER).agg((first(SalesOrderVariables.SHIPPING_AMOUNT) + first(SalesOrderVariables.COD_CHARGE) + first(SalesOrderVariables.GW_AMOUNT) + sum(SalesOrderItemVariables.PAID_PRICE) + sum(SalesOrderItemVariables.GIFTCARD_CREDITS_VALUE) + sum(SalesOrderItemVariables.STORE_CREDITS_VALUE) + sum(SalesOrderItemVariables.PAYBACK_CREDITS_VALUE)) as SalesOrderItemVariables.REVENUE, countDistinct(SalesOrderVariables.ID_SALES_ORDER) as SalesOrderVariables.ORDERS_COUNT)
    val newRdd = resultDF.rdd
    val schema = StructType(Array(StructField(SalesOrderVariables.FK_CUSTOMER, IntegerType, true), StructField(SalesOrderItemVariables.REVENUE + domain, DecimalType.apply(16, 2), true), StructField(SalesOrderVariables.ORDERS_COUNT + domain, LongType, true)))
    val res = Spark.getSqlContext().createDataFrame(newRdd, schema)
    res.printSchema()
    res.show(5)
    res
  }

  def getRevenueDays(curr: DataFrame, prev: DataFrame, days: Int, day1: Int, day2: Int): DataFrame = {
    val bcCurr = Spark.getContext().broadcast(prev)
    val joinedData = prev.join(bcCurr.value, bcCurr.value(SalesOrderVariables.FK_CUSTOMER) === prev(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
    val res = joinedData.select(
      coalesce(
        prev(SalesOrderVariables.FK_CUSTOMER),
        bcCurr.value(SalesOrderVariables.FK_CUSTOMER)
      ) as SalesOrderVariables.FK_CUSTOMER,
      joinedData(SalesOrderItemVariables.ORDERS_COUNT + "_" + days) - joinedData(SalesOrderItemVariables.ORDERS_COUNT) as SalesOrderItemVariables.ORDERS_COUNT + "_" + days,
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_APP + "_" + days) - joinedData(SalesOrderItemVariables.ORDERS_COUNT_APP) as SalesOrderItemVariables.ORDERS_COUNT_APP + "_" + days,
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_WEB + "_" + days) - joinedData(SalesOrderItemVariables.ORDERS_COUNT_WEB) as SalesOrderItemVariables.ORDERS_COUNT_WEB + "_" + days,
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_MWEB + "_" + days) - joinedData(SalesOrderItemVariables.ORDERS_COUNT_MWEB) as SalesOrderItemVariables.ORDERS_COUNT_MWEB + "_" + days,
      joinedData(SalesOrderItemVariables.REVENUE + "_" + days) - joinedData(SalesOrderItemVariables.REVENUE) as SalesOrderItemVariables.REVENUE + "_" + days,
      joinedData(SalesOrderItemVariables.REVENUE_APP + "_" + days) - joinedData(SalesOrderItemVariables.REVENUE_APP) as SalesOrderItemVariables.REVENUE_APP + "_" + days,
      joinedData(SalesOrderItemVariables.REVENUE_WEB + "_" + days) - joinedData(SalesOrderItemVariables.REVENUE_WEB) as SalesOrderItemVariables.REVENUE_WEB + "_" + days,
      joinedData(SalesOrderItemVariables.REVENUE_MWEB + "_" + days) - joinedData(SalesOrderItemVariables.REVENUE_MWEB) as SalesOrderItemVariables.REVENUE_MWEB + "_" + days,
      joinedData(SalesOrderItemVariables.ORDERS_COUNT + "_" + day1),
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_APP + "_" + day1),
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_WEB + "_" + day1),
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_MWEB + "_" + day1),
      joinedData(SalesOrderItemVariables.REVENUE + "_" + day1),
      joinedData(SalesOrderItemVariables.REVENUE_APP + "_" + day1),
      joinedData(SalesOrderItemVariables.REVENUE_WEB + "_" + day1),
      joinedData(SalesOrderItemVariables.REVENUE_MWEB + "_" + day1),
      joinedData(SalesOrderItemVariables.ORDERS_COUNT + "_" + day2),
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_APP + "_" + day2),
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_WEB + "_" + day2),
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_MWEB + "_" + day2),
      joinedData(SalesOrderItemVariables.REVENUE + "_" + day2),
      joinedData(SalesOrderItemVariables.REVENUE_APP + "_" + day2),
      joinedData(SalesOrderItemVariables.REVENUE_WEB + "_" + day2),
      joinedData(SalesOrderItemVariables.REVENUE_MWEB + "_" + day2),
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_LIFE),
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_APP_LIFE),
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_WEB_LIFE),
      joinedData(SalesOrderItemVariables.ORDERS_COUNT_MWEB_LIFE),
      joinedData(SalesOrderItemVariables.REVENUE_LIFE),
      joinedData(SalesOrderItemVariables.REVENUE_APP_LIFE),
      joinedData(SalesOrderItemVariables.REVENUE_WEB_LIFE),
      joinedData(SalesOrderItemVariables.REVENUE_MWEB_LIFE)
    )
    res.printSchema()
    res.show(5)
    res
  }

  /**
   * def main(args: Array[String]) {
   * val conf = new SparkConf().setAppName("SparkExamples")
   * Spark.init(conf)
   * val df1 = JsonUtils.readFromJson("sales_order_item", "sales_order_item1", OrderVarSchema.salesOrderItem)
   * df1.collect.foreach(println)
   * val  x = getSucessfulOrders(df1)
   *
   * df1.collect().foreach(println)
   *
   * }
   */

}
