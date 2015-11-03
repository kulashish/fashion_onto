package com.jabong.dap.model.order.variables

import com.jabong.dap.common.time.TimeConstants
import com.jabong.dap.common.{ Utils, Spark }
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.status.OrderStatus
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.model.product.itr.variables.ITR
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Created by mubarak on 3/7/15.
 */
object SalesOrderItem {

  /**
   * Creates order_count(app,web,mweb) and Revenue(app,web,mweb)
   * @param salesOrderJoined sales_order, sales_order_item tables joined data
   * @return Dataframe with the latest values for orders_count and ravenue for each customer
   */

  def getRevenueOrdersCount(salesOrderJoined: DataFrame, prevFull: DataFrame, before7: DataFrame, before30: DataFrame, before90: DataFrame): (DataFrame, DataFrame) = {
    val salesJoinedDF = salesOrderJoined
      .select(
        salesOrderJoined(SalesOrderVariables.FK_CUSTOMER).cast(LongType) as SalesOrderVariables.FK_CUSTOMER,
        salesOrderJoined(SalesOrderVariables.COD_CHARGE),
        salesOrderJoined(SalesOrderVariables.GW_AMOUNT),
        salesOrderJoined(SalesOrderVariables.SHIPPING_AMOUNT),
        salesOrderJoined(SalesOrderVariables.ID_SALES_ORDER).cast(LongType) as SalesOrderVariables.ID_SALES_ORDER,
        salesOrderJoined(SalesOrderItemVariables.GIFTCARD_CREDITS_VALUE),
        salesOrderJoined(SalesOrderItemVariables.PAYBACK_CREDITS_VALUE),
        salesOrderJoined(SalesOrderItemVariables.PAID_PRICE),
        salesOrderJoined(SalesOrderItemVariables.STORE_CREDITS_VALUE),
        salesOrderJoined(SalesOrderVariables.DOMAIN),
        salesOrderJoined(SalesOrderVariables.CREATED_AT)
      )
    val appOrders = salesJoinedDF.filter(SalesOrderItemVariables.FILTER_APP)
    val webOrders = salesJoinedDF.filter(SalesOrderItemVariables.FILTER_WEB)
    val mWebOrders = salesJoinedDF.filter(SalesOrderItemVariables.FILTER_MWEB)
    val app = getRevenueOrders(appOrders, "_app")
    val web = getRevenueOrders(webOrders, "_web")
    val mWeb = getRevenueOrders(mWebOrders, "_mweb")
    val salesVariablesIncr = joinDataFrames(app, web, mWeb)

    val mergedData = merge(salesVariablesIncr, prevFull)

    val res7 = getRevenueDays(before7, mergedData, 7, 30, 90)

    val res30 = getRevenueDays(before30, res7, 30, 7, 90)

    val salesVariablesFull = getRevenueDays(before90, res30, 90, 7, 30)

    (salesVariablesIncr, salesVariablesFull)

  }

  //TODO change this method
  def addColumnsforBeforeData(salesRevenue: DataFrame): DataFrame = {
    salesRevenue.withColumn(SalesOrderItemVariables.ORDERS_COUNT_7, salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP) - salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP))
      .withColumn(SalesOrderItemVariables.ORDERS_COUNT_APP_7, salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP) - salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP))
      .withColumn(SalesOrderItemVariables.ORDERS_COUNT_WEB_7, salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP) - salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP))
      .withColumn(SalesOrderItemVariables.ORDERS_COUNT_MWEB_7, salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP) - salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP))
      .withColumn(SalesOrderItemVariables.REVENUE_7, salesRevenue(SalesOrderItemVariables.REVENUE_APP) - salesRevenue(SalesOrderItemVariables.REVENUE_APP))
      .withColumn(SalesOrderItemVariables.REVENUE_APP_7, salesRevenue(SalesOrderItemVariables.REVENUE_APP) - salesRevenue(SalesOrderItemVariables.REVENUE_APP))
      .withColumn(SalesOrderItemVariables.REVENUE_WEB_7, salesRevenue(SalesOrderItemVariables.REVENUE_APP) - salesRevenue(SalesOrderItemVariables.REVENUE_APP))
      .withColumn(SalesOrderItemVariables.REVENUE_MWEB_7, salesRevenue(SalesOrderItemVariables.REVENUE_APP) - salesRevenue(SalesOrderItemVariables.REVENUE_APP))
      .withColumn(SalesOrderItemVariables.ORDERS_COUNT_30, salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP) - salesRevenue(SalesOrderItemVariables.ORDERS_COUNT))
      .withColumn(SalesOrderItemVariables.ORDERS_COUNT_APP_30, salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP) - salesRevenue(SalesOrderItemVariables.ORDERS_COUNT))
      .withColumn(SalesOrderItemVariables.ORDERS_COUNT_WEB_30, salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP) - salesRevenue(SalesOrderItemVariables.ORDERS_COUNT))
      .withColumn(SalesOrderItemVariables.ORDERS_COUNT_MWEB_30, salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP) - salesRevenue(SalesOrderItemVariables.ORDERS_COUNT))
      .withColumn(SalesOrderItemVariables.REVENUE_30, salesRevenue(SalesOrderItemVariables.REVENUE_APP) - salesRevenue(SalesOrderItemVariables.REVENUE_APP))
      .withColumn(SalesOrderItemVariables.REVENUE_APP_30, salesRevenue(SalesOrderItemVariables.REVENUE_APP) - salesRevenue(SalesOrderItemVariables.REVENUE_APP))
      .withColumn(SalesOrderItemVariables.REVENUE_WEB_30, salesRevenue(SalesOrderItemVariables.REVENUE_APP) - salesRevenue(SalesOrderItemVariables.REVENUE_APP))
      .withColumn(SalesOrderItemVariables.REVENUE_MWEB_30, salesRevenue(SalesOrderItemVariables.REVENUE_APP) - salesRevenue(SalesOrderItemVariables.REVENUE_APP))
      .withColumn(SalesOrderItemVariables.ORDERS_COUNT_90, salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP) - salesRevenue(SalesOrderItemVariables.ORDERS_COUNT))
      .withColumn(SalesOrderItemVariables.ORDERS_COUNT_APP_90, salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP) - salesRevenue(SalesOrderItemVariables.ORDERS_COUNT))
      .withColumn(SalesOrderItemVariables.ORDERS_COUNT_WEB_90, salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP) - salesRevenue(SalesOrderItemVariables.ORDERS_COUNT))
      .withColumn(SalesOrderItemVariables.ORDERS_COUNT_MWEB_90, salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP) - salesRevenue(SalesOrderItemVariables.ORDERS_COUNT))
      .withColumn(SalesOrderItemVariables.REVENUE_90, salesRevenue(SalesOrderItemVariables.REVENUE_APP) - salesRevenue(SalesOrderItemVariables.REVENUE_APP))
      .withColumn(SalesOrderItemVariables.REVENUE_APP_90, salesRevenue(SalesOrderItemVariables.REVENUE_APP) - salesRevenue(SalesOrderItemVariables.REVENUE_APP))
      .withColumn(SalesOrderItemVariables.REVENUE_WEB_90, salesRevenue(SalesOrderItemVariables.REVENUE_APP) - salesRevenue(SalesOrderItemVariables.REVENUE_APP))
      .withColumn(SalesOrderItemVariables.REVENUE_MWEB_90, salesRevenue(SalesOrderItemVariables.REVENUE_APP) - salesRevenue(SalesOrderItemVariables.REVENUE_APP))
      .withColumnRenamed(SalesOrderItemVariables.ORDERS_COUNT_APP, SalesOrderItemVariables.ORDERS_COUNT_APP_LIFE)
      .withColumnRenamed(SalesOrderItemVariables.ORDERS_COUNT_WEB, SalesOrderItemVariables.ORDERS_COUNT_WEB_LIFE)
      .withColumnRenamed(SalesOrderItemVariables.ORDERS_COUNT_MWEB, SalesOrderItemVariables.ORDERS_COUNT_MWEB_LIFE)
      .withColumnRenamed(SalesOrderItemVariables.REVENUE_APP, SalesOrderItemVariables.REVENUE_APP_LIFE)
      .withColumnRenamed(SalesOrderItemVariables.REVENUE_WEB, SalesOrderItemVariables.REVENUE_WEB_LIFE)
      .withColumnRenamed(SalesOrderItemVariables.REVENUE_MWEB, SalesOrderItemVariables.REVENUE_MWEB_LIFE)
      .withColumnRenamed(SalesOrderItemVariables.REVENUE, SalesOrderItemVariables.REVENUE_LIFE)
      .withColumnRenamed(SalesOrderItemVariables.ORDERS_COUNT, SalesOrderItemVariables.ORDERS_COUNT_LIFE)

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
        coalesce(bcapp(SalesOrderVariables.FK_CUSTOMER), bcweb(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
        bcweb(SalesOrderItemVariables.ORDERS_COUNT_WEB),
        bcweb(SalesOrderItemVariables.REVENUE_WEB),
        bcapp(SalesOrderItemVariables.ORDERS_COUNT_APP),
        bcapp(SalesOrderItemVariables.REVENUE_APP),
        coalesce(bcapp(SalesOrderVariables.LAST_ORDER_DATE), bcweb(SalesOrderVariables.LAST_ORDER_DATE)) as SalesOrderVariables.LAST_ORDER_DATE
      )
    val joinedData = appJoined.join(bcmweb, bcmweb(SalesOrderVariables.FK_CUSTOMER) === appJoined(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER).
      select(
        coalesce(bcmweb(SalesOrderVariables.FK_CUSTOMER), appJoined(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
        appJoined(SalesOrderItemVariables.ORDERS_COUNT_WEB),
        appJoined(SalesOrderItemVariables.REVENUE_WEB),
        appJoined(SalesOrderItemVariables.ORDERS_COUNT_APP),
        appJoined(SalesOrderItemVariables.REVENUE_APP),
        bcmweb(SalesOrderItemVariables.ORDERS_COUNT_MWEB),
        bcmweb(SalesOrderItemVariables.REVENUE_MWEB),
        coalesce(bcmweb(SalesOrderVariables.LAST_ORDER_DATE), appJoined(SalesOrderVariables.LAST_ORDER_DATE)) as SalesOrderVariables.LAST_ORDER_DATE
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
    if (null == full) {
      return addColumnsforBeforeData(inc)
    }
    val bcInc = Spark.getContext().broadcast(inc).value
    val res = full.join(bcInc, bcInc(SalesOrderVariables.FK_CUSTOMER) === full(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
      .select(
        coalesce(full(SalesOrderVariables.FK_CUSTOMER), bcInc(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
        full(SalesOrderItemVariables.ORDERS_COUNT_LIFE) + bcInc(SalesOrderItemVariables.ORDERS_COUNT) as SalesOrderItemVariables.ORDERS_COUNT_LIFE,
        full(SalesOrderItemVariables.ORDERS_COUNT_APP_LIFE) + bcInc(SalesOrderItemVariables.ORDERS_COUNT_APP) as SalesOrderItemVariables.ORDERS_COUNT_APP_LIFE,
        full(SalesOrderItemVariables.ORDERS_COUNT_WEB_LIFE) + bcInc(SalesOrderItemVariables.ORDERS_COUNT_WEB) as SalesOrderItemVariables.ORDERS_COUNT_WEB_LIFE,
        full(SalesOrderItemVariables.ORDERS_COUNT_MWEB_LIFE) + bcInc(SalesOrderItemVariables.ORDERS_COUNT_MWEB) as SalesOrderItemVariables.ORDERS_COUNT_MWEB_LIFE,
        full(SalesOrderItemVariables.REVENUE_LIFE) + bcInc(SalesOrderItemVariables.REVENUE) as SalesOrderItemVariables.REVENUE_LIFE,
        full(SalesOrderItemVariables.REVENUE_APP_LIFE) + bcInc(SalesOrderItemVariables.REVENUE_APP) as SalesOrderItemVariables.REVENUE_APP_LIFE,
        full(SalesOrderItemVariables.REVENUE_WEB_LIFE) + bcInc(SalesOrderItemVariables.REVENUE_WEB) as SalesOrderItemVariables.REVENUE_WEB_LIFE,
        full(SalesOrderItemVariables.REVENUE_MWEB_LIFE) + bcInc(SalesOrderItemVariables.REVENUE_MWEB) as SalesOrderItemVariables.REVENUE_MWEB_LIFE,
        full(SalesOrderItemVariables.ORDERS_COUNT_7) + bcInc(SalesOrderItemVariables.ORDERS_COUNT) as SalesOrderItemVariables.ORDERS_COUNT_7,
        full(SalesOrderItemVariables.ORDERS_COUNT_APP_7) + bcInc(SalesOrderItemVariables.ORDERS_COUNT_APP) as SalesOrderItemVariables.ORDERS_COUNT_APP_7,
        full(SalesOrderItemVariables.ORDERS_COUNT_WEB_7) + bcInc(SalesOrderItemVariables.ORDERS_COUNT_WEB) as SalesOrderItemVariables.ORDERS_COUNT_WEB_7,
        full(SalesOrderItemVariables.ORDERS_COUNT_MWEB_7) + bcInc(SalesOrderItemVariables.ORDERS_COUNT_MWEB) as SalesOrderItemVariables.ORDERS_COUNT_MWEB_7,
        full(SalesOrderItemVariables.REVENUE_7) + bcInc(SalesOrderItemVariables.REVENUE) as SalesOrderItemVariables.REVENUE_7,
        full(SalesOrderItemVariables.REVENUE_APP_7) + bcInc(SalesOrderItemVariables.REVENUE_APP) as SalesOrderItemVariables.REVENUE_APP_7,
        full(SalesOrderItemVariables.REVENUE_WEB_7) + bcInc(SalesOrderItemVariables.REVENUE_WEB) as SalesOrderItemVariables.REVENUE_WEB_7,
        full(SalesOrderItemVariables.REVENUE_MWEB_7) + bcInc(SalesOrderItemVariables.REVENUE_MWEB) as SalesOrderItemVariables.REVENUE_MWEB_7,
        full(SalesOrderItemVariables.ORDERS_COUNT_30) + bcInc(SalesOrderItemVariables.ORDERS_COUNT) as SalesOrderItemVariables.ORDERS_COUNT_30,
        full(SalesOrderItemVariables.ORDERS_COUNT_APP_30) + bcInc(SalesOrderItemVariables.ORDERS_COUNT_APP) as SalesOrderItemVariables.ORDERS_COUNT_APP_30,
        full(SalesOrderItemVariables.ORDERS_COUNT_WEB_30) + bcInc(SalesOrderItemVariables.ORDERS_COUNT_WEB) as SalesOrderItemVariables.ORDERS_COUNT_WEB_30,
        full(SalesOrderItemVariables.ORDERS_COUNT_MWEB_30) + bcInc(SalesOrderItemVariables.ORDERS_COUNT_MWEB) as SalesOrderItemVariables.ORDERS_COUNT_MWEB_30,
        full(SalesOrderItemVariables.REVENUE_30) + bcInc(SalesOrderItemVariables.REVENUE) as SalesOrderItemVariables.REVENUE_30,
        full(SalesOrderItemVariables.REVENUE_APP_30) + bcInc(SalesOrderItemVariables.REVENUE_APP) as SalesOrderItemVariables.REVENUE_APP_30,
        full(SalesOrderItemVariables.REVENUE_WEB_30) + bcInc(SalesOrderItemVariables.REVENUE_WEB) as SalesOrderItemVariables.REVENUE_WEB_30,
        full(SalesOrderItemVariables.REVENUE_MWEB_30) + bcInc(SalesOrderItemVariables.REVENUE_MWEB) as SalesOrderItemVariables.REVENUE_MWEB_30,
        full(SalesOrderItemVariables.ORDERS_COUNT_90) + bcInc(SalesOrderItemVariables.ORDERS_COUNT) as SalesOrderItemVariables.ORDERS_COUNT_90,
        full(SalesOrderItemVariables.ORDERS_COUNT_APP_90) + bcInc(SalesOrderItemVariables.ORDERS_COUNT_APP) as SalesOrderItemVariables.ORDERS_COUNT_APP_90,
        full(SalesOrderItemVariables.ORDERS_COUNT_WEB_90) + bcInc(SalesOrderItemVariables.ORDERS_COUNT_WEB) as SalesOrderItemVariables.ORDERS_COUNT_WEB_90,
        full(SalesOrderItemVariables.ORDERS_COUNT_MWEB_90) + bcInc(SalesOrderItemVariables.ORDERS_COUNT_MWEB) as SalesOrderItemVariables.ORDERS_COUNT_MWEB_90,
        full(SalesOrderItemVariables.REVENUE_90) + bcInc(SalesOrderItemVariables.REVENUE) as SalesOrderItemVariables.REVENUE_90,
        full(SalesOrderItemVariables.REVENUE_APP_90) + bcInc(SalesOrderItemVariables.REVENUE_APP) as SalesOrderItemVariables.REVENUE_APP_90,
        full(SalesOrderItemVariables.REVENUE_WEB_90) + bcInc(SalesOrderItemVariables.REVENUE_WEB) as SalesOrderItemVariables.REVENUE_WEB_90,
        full(SalesOrderItemVariables.REVENUE_MWEB_90) + bcInc(SalesOrderItemVariables.REVENUE_MWEB) as SalesOrderItemVariables.REVENUE_MWEB_90,
        coalesce(bcInc(SalesOrderVariables.LAST_ORDER_DATE), full(SalesOrderVariables.LAST_ORDER_DATE)) as SalesOrderVariables.LAST_ORDER_DATE
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

    (ordersCount, successOrdersUnion, favBrandIncr, favBrandUnion)
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
      mostPrefBrandUnion = dfFavBrandCalcPrevFull.unionAll(mostPrefBrandIncr).dropDuplicates()
      val fkCustList = mostPrefBrandJoinedIncr.select(SalesOrderVariables.FK_CUSTOMER).distinct
      mostPrefBrandIncr = mostPrefBrandUnion.join(fkCustList,
        fkCustList(SalesOrderVariables.FK_CUSTOMER) === mostPrefBrandUnion(SalesOrderVariables.FK_CUSTOMER))
        .select(
          mostPrefBrandUnion(SalesOrderVariables.FK_CUSTOMER),
          mostPrefBrandUnion(SalesOrderVariables.ID_SALES_ORDER),
          mostPrefBrandUnion(SalesOrderItemVariables.SKU),
          mostPrefBrandUnion(ProductVariables.BRAND),
          mostPrefBrandUnion(ProductVariables.SPECIAL_PRICE))

      //      mostPrefBrandIncr = mostPrefBrandUnion.filter(mostPrefBrandUnion(SalesOrderVariables.FK_CUSTOMER).contains(mostPrefBrandJoinedIncr(SalesOrderVariables.FK_CUSTOMER)))
    }

    val SUM_SPECIAL_PRICE = "sum_special_price"
    val COUNT_BRAND = "count_brand"

    val favBrandIncr = (mostPrefBrandIncr.groupBy(SalesOrderVariables.FK_CUSTOMER, ProductVariables.BRAND)
      .agg(count(ProductVariables.BRAND) as COUNT_BRAND, sum(ProductVariables.SPECIAL_PRICE) as SUM_SPECIAL_PRICE)
      .sort(COUNT_BRAND, SUM_SPECIAL_PRICE)
      .groupBy(SalesOrderVariables.FK_CUSTOMER)
      .agg(last(ProductVariables.BRAND) as SalesOrderItemVariables.FAV_BRAND)).cache()

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
    val ordersCount = (newOrders.groupBy(SalesOrderVariables.FK_CUSTOMER)
      .agg(count("STATUS") as SalesOrderItemVariables.ORDERS_COUNT_SUCCESSFUL)).cache()

    (ordersCount, successOrdersUnion)
  }

  /**
   * Calculates revenue, orders_count per customer for the input dataframe
   * @param salesOrderItem
   * @param domain
   * @return dataframe with the revnue and orders count
   */
  def getRevenueOrders(salesOrderItem: DataFrame, domain: String): DataFrame = {
    val resultDF = salesOrderItem.groupBy(SalesOrderVariables.FK_CUSTOMER).agg((first(SalesOrderVariables.SHIPPING_AMOUNT) + first(SalesOrderVariables.COD_CHARGE) + first(SalesOrderVariables.GW_AMOUNT) + sum(SalesOrderItemVariables.PAID_PRICE) + sum(SalesOrderItemVariables.GIFTCARD_CREDITS_VALUE) + sum(SalesOrderItemVariables.STORE_CREDITS_VALUE) + sum(SalesOrderItemVariables.PAYBACK_CREDITS_VALUE)) as SalesOrderItemVariables.REVENUE, countDistinct(SalesOrderVariables.ID_SALES_ORDER) as SalesOrderVariables.ORDERS_COUNT, first(SalesOrderVariables.CREATED_AT) as SalesOrderVariables.CREATED_AT)
    val newRdd = resultDF.rdd
    val schema = StructType(Array(StructField(SalesOrderVariables.FK_CUSTOMER, LongType, true), StructField(SalesOrderItemVariables.REVENUE + domain, DecimalType.apply(16, 2), true), StructField(SalesOrderVariables.ORDERS_COUNT + domain, LongType, true), StructField(SalesOrderVariables.LAST_ORDER_DATE, TimestampType, true)))
    val res = Spark.getSqlContext().createDataFrame(newRdd, schema)
    res.printSchema()
    res.show(5)
    res
  }

  def getRevenueDays(curr: DataFrame, prev: DataFrame, days: Int, day1: Int, day2: Int): DataFrame = {
    if (null == curr) {
      prev.printSchema()
      prev.show(5)
      return prev
    }
    val bcCurr = Spark.getContext().broadcast(prev).value
    val joinedData = prev.join(bcCurr, bcCurr(SalesOrderVariables.FK_CUSTOMER) === prev(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
    val res = joinedData.select(
      coalesce(
        prev(SalesOrderVariables.FK_CUSTOMER),
        bcCurr(SalesOrderVariables.FK_CUSTOMER)
      ) as SalesOrderVariables.FK_CUSTOMER,
      prev(SalesOrderItemVariables.ORDERS_COUNT + "_" + days) - bcCurr(SalesOrderItemVariables.ORDERS_COUNT_LIFE) as SalesOrderItemVariables.ORDERS_COUNT + "_" + days,
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

  /*
   MIN_COUPON_VALUE_USED - coupon_money_value is available at order_item level and
   coupon_code is available at order level.
   Need to map coupon_code with sales_rule to get fk_sales_rule_set which is then mapped to
   sales_rule_set table to get discount_type.
   Need to take sum(coupon_money_value) group by id_sales_order where this discount_type is fixed.
   Then need to get the amount which is min. till date at order level.
   This is used as a proxy to discount score at times

   MAX_COUPON_VALUE_USED -
   AVG_COUPON_VALUE_USED -
   MIN_DISCOUNT_USED -

   MAX_DISCOUNT_USED - coupon_money_value is available at order_item level and coupon_code is available at order level.
   Need to map coupon_code with sales_rule to get fk_sales_rule_set which is then mapped
   to sales_rule_set table to get discount_type. Need to take discount_percentage wherever discount_type=percent.
   Then need to get the percentage which is max. till date at order level. This is used as a proxy to discount score at times
   AVERAGE_DISCOUNT_USED -> coupon_money_value is available at order_item level and
   coupon_code is available at order level.
   Need to map coupon_code with sales_rule to
   get fk_sales_rule_set which is then mapped to
   sales_rule_set table to get discount_type.
   Need to take discount_percentage wherever discount_type=percent.
   Then need to take average of all such percentage till date to arrive at avg_discount_used.
   This is used as a proxy to discount score at times

   */
  def getCouponDisc(salesOrder: DataFrame, salesRuleFull: DataFrame, salesRuleSet: DataFrame, prevFull: DataFrame): DataFrame = {
    val salesRuleJoined = salesOrder.join(salesRuleFull, salesOrder(SalesOrderVariables.COUPON_CODE) === salesRuleFull(SalesRuleVariables.CODE)).select(
      salesOrder(SalesOrderVariables.FK_CUSTOMER),
      salesOrder(SalesOrderVariables.ID_SALES_ORDER),
      salesRuleFull(SalesRuleVariables.CODE),
      salesRuleFull(SalesRuleVariables.FK_SALES_RULE_SET)
    )
    val salesSetJoined = salesRuleJoined.join(salesRuleSet, salesRuleSet(SalesRuleSetVariables.ID_SALES_RULE_SET) === salesRuleJoined(SalesRuleVariables.FK_SALES_RULE_SET))
      .select(salesRuleJoined(SalesOrderVariables.FK_CUSTOMER),
        salesRuleJoined(SalesOrderVariables.ID_SALES_ORDER),
        salesRuleJoined(SalesRuleVariables.CODE),
        salesRuleJoined(SalesRuleVariables.FK_SALES_RULE_SET),
        salesRuleSet(SalesRuleSetVariables.DISCOUNT_TYPE))

    val fixed = salesSetJoined.filter(salesSetJoined(SalesRuleSetVariables.DISCOUNT_TYPE) === "fixed")
    val percent = salesSetJoined.filter(salesSetJoined(SalesRuleSetVariables.DISCOUNT_TYPE) === "percent")
    var discCalc: DataFrame = null
    val disc = percent.groupBy(SalesOrderVariables.ID_SALES_ORDER)
      .agg(first(SalesOrderVariables.FK_CUSTOMER) as SalesOrderVariables.FK_CUSTOMER,
        min(SalesRuleSetVariables.DISCOUNT_PERCENTAGE) as SalesRuleSetVariables.MIN_DISCOUNT_USED,
        max(SalesRuleSetVariables.DISCOUNT_PERCENTAGE) as SalesRuleSetVariables.MAX_DISCOUNT_USED,
        sum(SalesRuleSetVariables.DISCOUNT_PERCENTAGE) as "discount_sum",
        count(SalesRuleSetVariables.DISCOUNT_PERCENTAGE) as "discount_count")
    val coup = fixed.groupBy(SalesOrderVariables.ID_SALES_ORDER)
      .agg(first(SalesOrderVariables.FK_CUSTOMER) as SalesOrderVariables.FK_CUSTOMER,
        min(SalesRuleSetVariables.DISCOUNT_AMOUNT_DEFAULT) as SalesRuleSetVariables.MIN_COUPON_VALUE_USED,
        max(SalesRuleSetVariables.DISCOUNT_AMOUNT_DEFAULT) as SalesRuleSetVariables.MAX_COUPON_VALUE_USED,
        sum(SalesRuleSetVariables.DISCOUNT_AMOUNT_DEFAULT) as "coupon_sum",
        count(SalesRuleSetVariables.DISCOUNT_AMOUNT_DEFAULT) as "coupon_count")

    val incr = disc.join(coup, coup(SalesOrderVariables.FK_CUSTOMER) === disc(SalesOrderVariables.FK_CUSTOMER))
      .select(coalesce(coup(SalesOrderVariables.FK_CUSTOMER), disc(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
        coup(SalesRuleSetVariables.MIN_COUPON_VALUE_USED),
        coup(SalesRuleSetVariables.MAX_COUPON_VALUE_USED),
        coup(SalesRuleSetVariables.COUPON_COUNT),
        coup(SalesRuleSetVariables.COUPON_SUM),
        disc(SalesRuleSetVariables.MIN_DISCOUNT_USED),
        disc(SalesRuleSetVariables.MAX_DISCOUNT_USED),
        disc(SalesRuleSetVariables.DISCOUNT_SUM),
        disc(SalesRuleSetVariables.DISCOUNT_COUNT)
      )

    incr
  }

  /*
   COUNT_OF_RET_ORDERS - 8,12,32,35,36,37,38 are returned orderitem states.
   Would need to check count of order_items at orderlevel which are in these states.
   Then would need to compare this again count of orderitems at orderlevel.
   Need to get final count of orders where returned orderitem count matches total orderitem count
   COUNT_OF_CNCLD_ORDERS - 14,15,16,23,25,26,27,28 are cancelled orderitem states.
   Would need to check count of order_items at orderlevel which are in these states.
   Then would need to compare this again count of orderitems at orderlevel.
   Need to get final count of orders where cancelled orderitem count matches total orderitem count
   COUNT_OF_INVLD_ORDERS - 10 is the only invalid orderitem state.
   Would need to check count of order_items at orderlevel which are in these states.
   Then would need to compare this again count of orderitems at orderlevel.
   Need to get final count of orders where invalid orderitem count matches total orderitem count
   */

  def getInvalidCancelOrders(salesOrderJoined: DataFrame): DataFrame = {
    val joinedMap = salesOrderJoined.select(salesOrderJoined(SalesOrderVariables.ID_SALES_ORDER),
      salesOrderJoined(SalesOrderVariables.FK_CUSTOMER),
      salesOrderJoined(SalesOrderItemVariables.FK_SALES_ORDER_ITEM_STATUS))
      .map(e => (e(0).asInstanceOf[LongType], e(1).asInstanceOf[LongType]) -> (e(2).asInstanceOf[Int]))
      .groupByKey()
    val orderType = joinedMap.map(e => (e._1, findOrderType(e._2.toList))).map(e => (e._1._1, e._1._2, e._2))

    val ordersDf = Spark.getSqlContext().createDataFrame(orderType).withColumnRenamed("_1", SalesOrderVariables.ID_SALES_ORDER)
      .withColumnRenamed("_2", SalesOrderVariables.FK_CUSTOMER)
      .withColumnRenamed("_3", "type")

    val canOrders = ordersDf.select(ordersDf(SalesOrderVariables.ID_SALES_ORDER),
      ordersDf(SalesOrderVariables.FK_CUSTOMER),
      when(ordersDf("type") === 10, 1).otherwise(0) as "invalid",
      when(ordersDf("type") === 20, 1).otherwise(0) as "cancel",
      when(ordersDf("type") === 30, 1).otherwise(0) as "return"
    )
    val res = canOrders.groupBy(SalesOrderVariables.FK_CUSTOMER)
      .agg(count("invalid") as SalesOrderItemVariables.COUNT_OF_INVLD_ORDERS,
        count("cancel") as SalesOrderItemVariables.COUNT_OF_CNCLD_ORDERS,
        count("return") as SalesOrderItemVariables.COUNT_OF_RET_ORDERS
      )
    res
  }

  def findOrderType(list: List[Int]): Int = {
    var f = 0
    var g = 0
    var h = 0
    list.foreach(
      e =>
        if (OrderStatus.INVALID != e) {
          f = 1
        } else if (!OrderStatus.CANCELLED_ARRAY.contains(e)) {
          g = 1
        } else if (!OrderStatus.RETURN_ARRAY.contains(e)) {
          h = 1
        }
    )
    if (f == 0) {
      10
    } else if (g == 0) {
      20
    } else if (h == 0) {
      30
    } else {
      0
    }

  }

  /*
  MAX_ORDER_BASKET_VALUE - max of sum(unit_price) at order level & customer level.
  we need sum of special price (which is unit_price) at order level.
  Need to retrieve this for order having max. sum of special price.
  MAX_ORDER_ITEM_VALUE - max(unit_price) group by fk_customer. need to join sales_order to sales_order_item on id_sales_order.
  Do max(unit_price)group by fk_customer to get max.sp paid till date by that’s customer
  AVG_ORDER_VALUE - avg of sum(unit_price) at order level for a customer.
  we need sum of special price (which is unit_price) at order level.
  Need to take avg of this sum(unit_price) for all orders placed till date by that customer
  AVG_ORDER_ITEM_VALUE - avg(unit_price) group by fk_customer.
  need to join sales_order to sales_order_item on id_sales_order.
  Do avg(unit_price)group by fk_customer to get avg.sp paid till date by that’s customer
  */

  def getOrderValue(salesOrderJoined: DataFrame): DataFrame = {
    val salesJoined = salesOrderJoined
      .select(
        salesOrderJoined(SalesOrderVariables.ID_SALES_ORDER),
        salesOrderJoined(SalesOrderVariables.FK_CUSTOMER),
        salesOrderJoined(SalesOrderItemVariables.UNIT_PRICE),
        salesOrderJoined(SalesOrderVariables.CREATED_AT)
      )
    val orderGrp = salesJoined.groupBy(SalesOrderVariables.FK_CUSTOMER, SalesOrderVariables.ID_SALES_ORDER)
      .agg(sum(SalesOrderItemVariables.UNIT_PRICE) as "basket_value",
        max(SalesOrderItemVariables.UNIT_PRICE) as "max_item",
        count(SalesOrderItemVariables.UNIT_PRICE) as "item_count",
        max(SalesOrderVariables.CREATED_AT) as SalesOrderVariables.CREATED_AT)
    val orderValue = orderGrp.groupBy(SalesOrderVariables.FK_CUSTOMER)
      .agg(max("basket_value") as SalesOrderVariables.MAX_ORDER_BASKET_VALUE,
        sum("basket_value") as SalesOrderVariables.SUM_BASKET_VALUE,
        count("basket_value") as SalesOrderVariables.COUNT_BASKET_VALUE,
        max("max_item") as SalesOrderVariables.MAX_ORDER_ITEM_VALUE,
        sum("item_count") as SalesOrderVariables.ORDER_ITEM_COUNT,
        max(SalesOrderVariables.CREATED_AT) as SalesOrderVariables.CREATED_AT)

    return orderValue
    /*     val orderValueFull =
        )
      */
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
