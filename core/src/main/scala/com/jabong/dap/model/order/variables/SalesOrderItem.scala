package com.jabong.dap.model.order.variables

import java.sql.Timestamp

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.status.OrderStatus
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.{ Debugging, Spark, Utils }
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Row }

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
      .withColumn(SalesOrderItemVariables.ORDERS_COUNT_30, salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP) - salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP))
      .withColumn(SalesOrderItemVariables.ORDERS_COUNT_APP_30, salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP) - salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP))
      .withColumn(SalesOrderItemVariables.ORDERS_COUNT_WEB_30, salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP) - salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP))
      .withColumn(SalesOrderItemVariables.ORDERS_COUNT_MWEB_30, salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP) - salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP))
      .withColumn(SalesOrderItemVariables.REVENUE_30, salesRevenue(SalesOrderItemVariables.REVENUE_APP) - salesRevenue(SalesOrderItemVariables.REVENUE_APP))
      .withColumn(SalesOrderItemVariables.REVENUE_APP_30, salesRevenue(SalesOrderItemVariables.REVENUE_APP) - salesRevenue(SalesOrderItemVariables.REVENUE_APP))
      .withColumn(SalesOrderItemVariables.REVENUE_WEB_30, salesRevenue(SalesOrderItemVariables.REVENUE_APP) - salesRevenue(SalesOrderItemVariables.REVENUE_APP))
      .withColumn(SalesOrderItemVariables.REVENUE_MWEB_30, salesRevenue(SalesOrderItemVariables.REVENUE_APP) - salesRevenue(SalesOrderItemVariables.REVENUE_APP))
      .withColumn(SalesOrderItemVariables.ORDERS_COUNT_90, salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP) - salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP))
      .withColumn(SalesOrderItemVariables.ORDERS_COUNT_APP_90, salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP) - salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP))
      .withColumn(SalesOrderItemVariables.ORDERS_COUNT_WEB_90, salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP) - salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP))
      .withColumn(SalesOrderItemVariables.ORDERS_COUNT_MWEB_90, salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP) - salesRevenue(SalesOrderItemVariables.ORDERS_COUNT_APP))
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
    val appJoined = web.join(app, app(SalesOrderVariables.FK_CUSTOMER) === web(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER).
      select(
        coalesce(app(SalesOrderVariables.FK_CUSTOMER), web(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
        web(SalesOrderItemVariables.ORDERS_COUNT_WEB),
        web(SalesOrderItemVariables.REVENUE_WEB),
        app(SalesOrderItemVariables.ORDERS_COUNT_APP),
        app(SalesOrderItemVariables.REVENUE_APP),
        coalesce(app(SalesOrderVariables.LAST_ORDER_DATE), web(SalesOrderVariables.LAST_ORDER_DATE)) as SalesOrderVariables.LAST_ORDER_DATE
      )
    val joinedData = appJoined.join(mWeb, mWeb(SalesOrderVariables.FK_CUSTOMER) === appJoined(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER).
      select(
        coalesce(mWeb(SalesOrderVariables.FK_CUSTOMER), appJoined(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
        appJoined(SalesOrderItemVariables.ORDERS_COUNT_WEB),
        appJoined(SalesOrderItemVariables.REVENUE_WEB),
        appJoined(SalesOrderItemVariables.ORDERS_COUNT_APP),
        appJoined(SalesOrderItemVariables.REVENUE_APP),
        mWeb(SalesOrderItemVariables.ORDERS_COUNT_MWEB),
        mWeb(SalesOrderItemVariables.REVENUE_MWEB),
        coalesce(mWeb(SalesOrderVariables.LAST_ORDER_DATE), appJoined(SalesOrderVariables.LAST_ORDER_DATE)) as SalesOrderVariables.LAST_ORDER_DATE
      )
    val joinedFill = joinedData.na.fill(Map(
      SalesOrderItemVariables.ORDERS_COUNT_APP -> 0,
      SalesOrderItemVariables.ORDERS_COUNT_WEB -> 0,
      SalesOrderItemVariables.ORDERS_COUNT_MWEB -> 0,
      SalesOrderItemVariables.REVENUE_APP -> 0.0,
      SalesOrderItemVariables.REVENUE_MWEB -> 0.0,
      SalesOrderItemVariables.REVENUE_WEB -> 0.0
    ))
    val res = joinedFill.withColumn(
      SalesOrderItemVariables.REVENUE,
      joinedFill(SalesOrderItemVariables.REVENUE_APP) + joinedFill(SalesOrderItemVariables.REVENUE_WEB) + joinedFill(SalesOrderItemVariables.REVENUE_MWEB)
    ).withColumn(
        SalesOrderItemVariables.ORDERS_COUNT,
        joinedFill(SalesOrderItemVariables.ORDERS_COUNT_APP) + joinedFill(SalesOrderItemVariables.ORDERS_COUNT_WEB) + joinedFill(SalesOrderItemVariables.ORDERS_COUNT_MWEB)
      )
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
    val incNew = inc.withColumnRenamed(SalesOrderVariables.FK_CUSTOMER, SalesOrderVariables.FK_CUSTOMER + "NEW")
      .withColumnRenamed(SalesOrderVariables.LAST_ORDER_DATE, SalesOrderVariables.LAST_ORDER_DATE + "NEW")
    val joined = full.join(incNew, incNew(SalesOrderVariables.FK_CUSTOMER + "NEW") === full(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
      .na.fill(Map(
        SalesOrderItemVariables.ORDERS_COUNT_APP -> 0,
        SalesOrderItemVariables.ORDERS_COUNT_WEB -> 0,
        SalesOrderItemVariables.ORDERS_COUNT_MWEB -> 0,
        SalesOrderItemVariables.ORDERS_COUNT -> 0,
        SalesOrderItemVariables.REVENUE_APP -> 0.0,
        SalesOrderItemVariables.REVENUE_MWEB -> 0.0,
        SalesOrderItemVariables.REVENUE_WEB -> 0.0,
        SalesOrderItemVariables.REVENUE -> 0.0
      ))
    val res = joined.select(
      coalesce(joined(SalesOrderVariables.FK_CUSTOMER), joined(SalesOrderVariables.FK_CUSTOMER + "NEW")) as SalesOrderVariables.FK_CUSTOMER,
      joined(SalesOrderItemVariables.ORDERS_COUNT_LIFE) + joined(SalesOrderItemVariables.ORDERS_COUNT) as SalesOrderItemVariables.ORDERS_COUNT_LIFE,
      joined(SalesOrderItemVariables.ORDERS_COUNT_APP_LIFE) + joined(SalesOrderItemVariables.ORDERS_COUNT_APP) as SalesOrderItemVariables.ORDERS_COUNT_APP_LIFE,
      joined(SalesOrderItemVariables.ORDERS_COUNT_WEB_LIFE) + joined(SalesOrderItemVariables.ORDERS_COUNT_WEB) as SalesOrderItemVariables.ORDERS_COUNT_WEB_LIFE,
      joined(SalesOrderItemVariables.ORDERS_COUNT_MWEB_LIFE) + joined(SalesOrderItemVariables.ORDERS_COUNT_MWEB) as SalesOrderItemVariables.ORDERS_COUNT_MWEB_LIFE,
      joined(SalesOrderItemVariables.REVENUE_LIFE) + joined(SalesOrderItemVariables.REVENUE) as SalesOrderItemVariables.REVENUE_LIFE,
      joined(SalesOrderItemVariables.REVENUE_APP_LIFE) + joined(SalesOrderItemVariables.REVENUE_APP) as SalesOrderItemVariables.REVENUE_APP_LIFE,
      joined(SalesOrderItemVariables.REVENUE_WEB_LIFE) + joined(SalesOrderItemVariables.REVENUE_WEB) as SalesOrderItemVariables.REVENUE_WEB_LIFE,
      joined(SalesOrderItemVariables.REVENUE_MWEB_LIFE) + joined(SalesOrderItemVariables.REVENUE_MWEB) as SalesOrderItemVariables.REVENUE_MWEB_LIFE,
      joined(SalesOrderItemVariables.ORDERS_COUNT_7) + joined(SalesOrderItemVariables.ORDERS_COUNT) as SalesOrderItemVariables.ORDERS_COUNT_7,
      joined(SalesOrderItemVariables.ORDERS_COUNT_APP_7) + joined(SalesOrderItemVariables.ORDERS_COUNT_APP) as SalesOrderItemVariables.ORDERS_COUNT_APP_7,
      joined(SalesOrderItemVariables.ORDERS_COUNT_WEB_7) + joined(SalesOrderItemVariables.ORDERS_COUNT_WEB) as SalesOrderItemVariables.ORDERS_COUNT_WEB_7,
      joined(SalesOrderItemVariables.ORDERS_COUNT_MWEB_7) + joined(SalesOrderItemVariables.ORDERS_COUNT_MWEB) as SalesOrderItemVariables.ORDERS_COUNT_MWEB_7,
      joined(SalesOrderItemVariables.REVENUE_7) + joined(SalesOrderItemVariables.REVENUE) as SalesOrderItemVariables.REVENUE_7,
      joined(SalesOrderItemVariables.REVENUE_APP_7) + joined(SalesOrderItemVariables.REVENUE_APP) as SalesOrderItemVariables.REVENUE_APP_7,
      joined(SalesOrderItemVariables.REVENUE_WEB_7) + joined(SalesOrderItemVariables.REVENUE_WEB) as SalesOrderItemVariables.REVENUE_WEB_7,
      joined(SalesOrderItemVariables.REVENUE_MWEB_7) + joined(SalesOrderItemVariables.REVENUE_MWEB) as SalesOrderItemVariables.REVENUE_MWEB_7,
      joined(SalesOrderItemVariables.ORDERS_COUNT_30) + joined(SalesOrderItemVariables.ORDERS_COUNT) as SalesOrderItemVariables.ORDERS_COUNT_30,
      joined(SalesOrderItemVariables.ORDERS_COUNT_APP_30) + joined(SalesOrderItemVariables.ORDERS_COUNT_APP) as SalesOrderItemVariables.ORDERS_COUNT_APP_30,
      joined(SalesOrderItemVariables.ORDERS_COUNT_WEB_30) + joined(SalesOrderItemVariables.ORDERS_COUNT_WEB) as SalesOrderItemVariables.ORDERS_COUNT_WEB_30,
      joined(SalesOrderItemVariables.ORDERS_COUNT_MWEB_30) + joined(SalesOrderItemVariables.ORDERS_COUNT_MWEB) as SalesOrderItemVariables.ORDERS_COUNT_MWEB_30,
      joined(SalesOrderItemVariables.REVENUE_30) + joined(SalesOrderItemVariables.REVENUE) as SalesOrderItemVariables.REVENUE_30,
      joined(SalesOrderItemVariables.REVENUE_APP_30) + joined(SalesOrderItemVariables.REVENUE_APP) as SalesOrderItemVariables.REVENUE_APP_30,
      joined(SalesOrderItemVariables.REVENUE_WEB_30) + joined(SalesOrderItemVariables.REVENUE_WEB) as SalesOrderItemVariables.REVENUE_WEB_30,
      joined(SalesOrderItemVariables.REVENUE_MWEB_30) + joined(SalesOrderItemVariables.REVENUE_MWEB) as SalesOrderItemVariables.REVENUE_MWEB_30,
      joined(SalesOrderItemVariables.ORDERS_COUNT_90) + joined(SalesOrderItemVariables.ORDERS_COUNT) as SalesOrderItemVariables.ORDERS_COUNT_90,
      joined(SalesOrderItemVariables.ORDERS_COUNT_APP_90) + joined(SalesOrderItemVariables.ORDERS_COUNT_APP) as SalesOrderItemVariables.ORDERS_COUNT_APP_90,
      joined(SalesOrderItemVariables.ORDERS_COUNT_WEB_90) + joined(SalesOrderItemVariables.ORDERS_COUNT_WEB) as SalesOrderItemVariables.ORDERS_COUNT_WEB_90,
      joined(SalesOrderItemVariables.ORDERS_COUNT_MWEB_90) + joined(SalesOrderItemVariables.ORDERS_COUNT_MWEB) as SalesOrderItemVariables.ORDERS_COUNT_MWEB_90,
      joined(SalesOrderItemVariables.REVENUE_90) + joined(SalesOrderItemVariables.REVENUE) as SalesOrderItemVariables.REVENUE_90,
      joined(SalesOrderItemVariables.REVENUE_APP_90) + joined(SalesOrderItemVariables.REVENUE_APP) as SalesOrderItemVariables.REVENUE_APP_90,
      joined(SalesOrderItemVariables.REVENUE_WEB_90) + joined(SalesOrderItemVariables.REVENUE_WEB) as SalesOrderItemVariables.REVENUE_WEB_90,
      joined(SalesOrderItemVariables.REVENUE_MWEB_90) + joined(SalesOrderItemVariables.REVENUE_MWEB) as SalesOrderItemVariables.REVENUE_MWEB_90,
      coalesce(joined(SalesOrderVariables.LAST_ORDER_DATE + "NEW"), joined(SalesOrderVariables.LAST_ORDER_DATE)) as SalesOrderVariables.LAST_ORDER_DATE
    )
    val newRdd = res.rdd
    val rev = Spark.getSqlContext().createDataFrame(newRdd, Schema.salesRev)
    rev
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
    res
  }

  def getRevenueDays(before: DataFrame, currFull: DataFrame, days: Int, day1: Int, day2: Int): DataFrame = {
    if (null == before) {
      return currFull
    }
    val befNew = before
      .withColumnRenamed(SalesOrderVariables.FK_CUSTOMER, SalesOrderVariables.FK_CUSTOMER + "_")
      .withColumnRenamed(SalesOrderVariables.LAST_ORDER_DATE, SalesOrderVariables.LAST_ORDER_DATE + "_")
    val joined = currFull.join(befNew, befNew(SalesOrderVariables.FK_CUSTOMER + "_") === currFull(SalesOrderVariables.FK_CUSTOMER), SQL.LEFT_OUTER)
      .na.fill(Map(
        SalesOrderItemVariables.ORDERS_COUNT_APP -> 0,
        SalesOrderItemVariables.ORDERS_COUNT_WEB -> 0,
        SalesOrderItemVariables.ORDERS_COUNT_MWEB -> 0,
        SalesOrderItemVariables.ORDERS_COUNT -> 0,
        SalesOrderItemVariables.REVENUE_APP -> 0.0,
        SalesOrderItemVariables.REVENUE_MWEB -> 0.0,
        SalesOrderItemVariables.REVENUE_WEB -> 0.0,
        SalesOrderItemVariables.REVENUE -> 0.0
      ))
    val res = joined.select(
      coalesce(joined(SalesOrderVariables.FK_CUSTOMER), joined(SalesOrderVariables.FK_CUSTOMER + "_")) as SalesOrderVariables.FK_CUSTOMER,
      joined(SalesOrderItemVariables.ORDERS_COUNT + "_" + days) - joined(SalesOrderItemVariables.ORDERS_COUNT) as SalesOrderItemVariables.ORDERS_COUNT + "_" + days,
      joined(SalesOrderItemVariables.ORDERS_COUNT_APP + "_" + days) - joined(SalesOrderItemVariables.ORDERS_COUNT_APP) as SalesOrderItemVariables.ORDERS_COUNT_APP + "_" + days,
      joined(SalesOrderItemVariables.ORDERS_COUNT_WEB + "_" + days) - joined(SalesOrderItemVariables.ORDERS_COUNT_WEB) as SalesOrderItemVariables.ORDERS_COUNT_WEB + "_" + days,
      joined(SalesOrderItemVariables.ORDERS_COUNT_MWEB + "_" + days) - joined(SalesOrderItemVariables.ORDERS_COUNT_MWEB) as SalesOrderItemVariables.ORDERS_COUNT_MWEB + "_" + days,
      joined(SalesOrderItemVariables.REVENUE + "_" + days) - joined(SalesOrderItemVariables.REVENUE) as SalesOrderItemVariables.REVENUE + "_" + days,
      joined(SalesOrderItemVariables.REVENUE_APP + "_" + days) - joined(SalesOrderItemVariables.REVENUE_APP) as SalesOrderItemVariables.REVENUE_APP + "_" + days,
      joined(SalesOrderItemVariables.REVENUE_WEB + "_" + days) - joined(SalesOrderItemVariables.REVENUE_WEB) as SalesOrderItemVariables.REVENUE_WEB + "_" + days,
      joined(SalesOrderItemVariables.REVENUE_MWEB + "_" + days) - joined(SalesOrderItemVariables.REVENUE_MWEB) as SalesOrderItemVariables.REVENUE_MWEB + "_" + days,
      joined(SalesOrderItemVariables.ORDERS_COUNT + "_" + day1),
      joined(SalesOrderItemVariables.ORDERS_COUNT_APP + "_" + day1),
      joined(SalesOrderItemVariables.ORDERS_COUNT_WEB + "_" + day1),
      joined(SalesOrderItemVariables.ORDERS_COUNT_MWEB + "_" + day1),
      joined(SalesOrderItemVariables.REVENUE + "_" + day1),
      joined(SalesOrderItemVariables.REVENUE_APP + "_" + day1),
      joined(SalesOrderItemVariables.REVENUE_WEB + "_" + day1),
      joined(SalesOrderItemVariables.REVENUE_MWEB + "_" + day1),
      joined(SalesOrderItemVariables.ORDERS_COUNT + "_" + day2),
      joined(SalesOrderItemVariables.ORDERS_COUNT_APP + "_" + day2),
      joined(SalesOrderItemVariables.ORDERS_COUNT_WEB + "_" + day2),
      joined(SalesOrderItemVariables.ORDERS_COUNT_MWEB + "_" + day2),
      joined(SalesOrderItemVariables.REVENUE + "_" + day2),
      joined(SalesOrderItemVariables.REVENUE_APP + "_" + day2),
      joined(SalesOrderItemVariables.REVENUE_WEB + "_" + day2),
      joined(SalesOrderItemVariables.REVENUE_MWEB + "_" + day2),
      joined(SalesOrderItemVariables.ORDERS_COUNT_LIFE),
      joined(SalesOrderItemVariables.ORDERS_COUNT_APP_LIFE),
      joined(SalesOrderItemVariables.ORDERS_COUNT_WEB_LIFE),
      joined(SalesOrderItemVariables.ORDERS_COUNT_MWEB_LIFE),
      joined(SalesOrderItemVariables.REVENUE_LIFE),
      joined(SalesOrderItemVariables.REVENUE_APP_LIFE),
      joined(SalesOrderItemVariables.REVENUE_WEB_LIFE),
      joined(SalesOrderItemVariables.REVENUE_MWEB_LIFE),
      joined(SalesOrderVariables.LAST_ORDER_DATE)
    )
    val newRdd = res
      .select(
        SalesOrderVariables.FK_CUSTOMER,
        SalesOrderItemVariables.ORDERS_COUNT_LIFE,
        SalesOrderItemVariables.ORDERS_COUNT_APP_LIFE,
        SalesOrderItemVariables.ORDERS_COUNT_WEB_LIFE,
        SalesOrderItemVariables.ORDERS_COUNT_MWEB_LIFE,
        SalesOrderItemVariables.REVENUE_LIFE,
        SalesOrderItemVariables.REVENUE_APP_LIFE,
        SalesOrderItemVariables.REVENUE_WEB_LIFE,
        SalesOrderItemVariables.REVENUE_MWEB_LIFE,
        SalesOrderItemVariables.ORDERS_COUNT_7,
        SalesOrderItemVariables.ORDERS_COUNT_APP_7,
        SalesOrderItemVariables.ORDERS_COUNT_WEB_7,
        SalesOrderItemVariables.ORDERS_COUNT_MWEB_7,
        SalesOrderItemVariables.REVENUE_7,
        SalesOrderItemVariables.REVENUE_APP_7,
        SalesOrderItemVariables.REVENUE_WEB_7,
        SalesOrderItemVariables.REVENUE_MWEB_7,
        SalesOrderItemVariables.ORDERS_COUNT_30,
        SalesOrderItemVariables.ORDERS_COUNT_APP_30,
        SalesOrderItemVariables.ORDERS_COUNT_WEB_30,
        SalesOrderItemVariables.ORDERS_COUNT_MWEB_30,
        SalesOrderItemVariables.REVENUE_30,
        SalesOrderItemVariables.REVENUE_APP_30,
        SalesOrderItemVariables.REVENUE_WEB_30,
        SalesOrderItemVariables.REVENUE_MWEB_30,
        SalesOrderItemVariables.ORDERS_COUNT_90,
        SalesOrderItemVariables.ORDERS_COUNT_APP_90,
        SalesOrderItemVariables.ORDERS_COUNT_WEB_90,
        SalesOrderItemVariables.ORDERS_COUNT_MWEB_90,
        SalesOrderItemVariables.REVENUE_90,
        SalesOrderItemVariables.REVENUE_APP_90,
        SalesOrderItemVariables.REVENUE_WEB_90,
        SalesOrderItemVariables.REVENUE_MWEB_90,
        SalesOrderVariables.LAST_ORDER_DATE
      ).rdd
    val rev = Spark.getSqlContext().createDataFrame(newRdd, Schema.salesRev)
    rev
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
  def getCouponDisc(salesOrder: DataFrame, salesRuleFull: DataFrame, salesRuleSet: DataFrame): DataFrame = {
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
        salesRuleSet(SalesRuleSetVariables.DISCOUNT_TYPE),
        salesRuleSet(SalesRuleSetVariables.DISCOUNT_PERCENTAGE),
        salesRuleSet(SalesRuleSetVariables.DISCOUNT_AMOUNT_DEFAULT))

    val fixed = salesSetJoined.filter(salesSetJoined(SalesRuleSetVariables.DISCOUNT_TYPE) === "fixed")
    val percent = salesSetJoined.filter(salesSetJoined(SalesRuleSetVariables.DISCOUNT_TYPE) === "percent")
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

  def getInvalidCancelOrders(salesOrderItemIncr: DataFrame, salesOrderFull: DataFrame, prevMap: DataFrame, incrDate: String): (DataFrame, DataFrame) = {
    val salesOrderJoined = salesOrderFull.drop(SalesOrderItemVariables.UPDATED_AT).join(salesOrderItemIncr, salesOrderFull(SalesOrderVariables.ID_SALES_ORDER) === salesOrderItemIncr(SalesOrderVariables.FK_SALES_ORDER), SQL.RIGHT_OUTER)
    val incrMap = salesOrderJoined.select(
      salesOrderJoined(SalesOrderVariables.FK_CUSTOMER),
      salesOrderJoined(SalesOrderVariables.ID_SALES_ORDER),
      salesOrderJoined(SalesOrderItemVariables.ID_SALES_ORDER_ITEM),
      salesOrderJoined(SalesOrderItemVariables.FK_SALES_ORDER_ITEM_STATUS).cast(IntegerType) as SalesOrderItemVariables.FK_SALES_ORDER_ITEM_STATUS,
      salesOrderJoined(SalesOrderItemVariables.UPDATED_AT))
      .map(e =>
        (e(0).asInstanceOf[Long] -> (e(1).asInstanceOf[Long], e(2).asInstanceOf[Long], e(3).asInstanceOf[Int], e(4).asInstanceOf[Timestamp]))).groupByKey()
    val ordersMapIncr = incrMap.map(e => (e._1, makeMap4mGroupedData(e._2.toList)))

    // println("ordersMapIncr Count", ordersMapIncr.count())

    val ordersIncrFlat = ordersMapIncr.map(e => Row(e._1, e._2._1, e._2._2))

    val orderIncr = Spark.getSqlContext().createDataFrame(ordersIncrFlat, Schema.salesItemStatus)

    Debugging.debug(orderIncr, "orderIncr")

    var joinedMap: DataFrame = null

    if (null == prevMap) {
      joinedMap = orderIncr
    } else {
      joinedMap = prevMap.join(orderIncr, prevMap(SalesOrderVariables.FK_CUSTOMER) === orderIncr(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
        .select(coalesce(orderIncr(SalesOrderVariables.FK_CUSTOMER), prevMap(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
          mergeMaps(orderIncr("order_status_map"), prevMap("order_status_map")) as "order_status_map",
          coalesce(orderIncr("last_order_updated_at"), prevMap("last_order_updated_at")) as "last_order_updated_at"
        )
    }

    Debugging.debug(joinedMap, "joinedMap")

    val incrData = Utils.getOneDayData(joinedMap, "last_order_updated_at", incrDate, TimeConstants.DATE_FORMAT_FOLDER)

    val orderStatusMap = incrData.map(e => (e(0).asInstanceOf[Long],
      countOrders(e(1).asInstanceOf[scala.collection.immutable.Map[Long, scala.collection.immutable.Map[Long, Int]]]),
      e(2).asInstanceOf[Timestamp]))

    val finalOrdersCount = orderStatusMap.map(e => Row(e._1, e._2._1, e._2._2, e._2._3, e._2._4, e._2._5, e._3))

    val res = Spark.getSqlContext().createDataFrame(finalOrdersCount, Schema.ordersCount)
    // println("res Count", res.count())

    (res, joinedMap)
  }

  val mergeMaps = udf((map1: scala.collection.immutable.Map[Long, scala.collection.immutable.Map[Long, Int]], map2: scala.collection.immutable.Map[Long, scala.collection.immutable.Map[Long, Int]]) => joinMaps(map1, map2))

  def joinMaps(map1: scala.collection.immutable.Map[Long, scala.collection.immutable.Map[Long, Int]], map2: scala.collection.immutable.Map[Long, scala.collection.immutable.Map[Long, Int]]): scala.collection.mutable.Map[Long, scala.collection.mutable.Map[Long, Int]] = {
    val full = scala.collection.mutable.Map[Long, scala.collection.mutable.Map[Long, Int]]()
    map2.keySet.foreach{
      orderId =>
        val itemMap = map2(orderId)
        val item = scala.collection.mutable.Map[Long, Int]()
        itemMap.keySet.foreach{
          ItemId =>
            item.put(ItemId, itemMap(ItemId))
        }
        full.put(orderId, item)
    }
    map1.keySet.foreach{
      orderId =>
        if (full.contains(orderId)) {
          val itemMapPrev = full(orderId)
          val itemMapNew = map1(orderId)
          itemMapNew.keySet.foreach{
            itemId =>
              if (itemMapPrev.contains(itemId)) {
                itemMapPrev.update(itemId, itemMapNew(itemId))
              } else {
                itemMapPrev.put(itemId, itemMapNew(itemId))
              }
          }
        } else {
          val itemMap = map1(orderId)
          val item = scala.collection.mutable.Map[Long, Int]()
          itemMap.keySet.foreach{
            ItemId =>
              item.put(ItemId, itemMap(ItemId))
          }
          full.put(orderId, item)
        }

    }
    full
  }

  def makeMap4mGroupedData(list: List[(Long, Long, Int, Timestamp)]): (scala.collection.mutable.Map[Long, scala.collection.mutable.Map[Long, Int]], Timestamp) = {
    val map = scala.collection.mutable.Map[Long, scala.collection.mutable.Map[Long, Int]]()

    var maxDate: Timestamp = TimeUtils.MIN_TIMESTAMP
    if (list.length > 0) {
      maxDate = list(0)._4
    }
    list.foreach{
      val innerMap = scala.collection.mutable.Map[Long, Int]()
      e =>
        if (maxDate.after(list(0)._4))
          maxDate = list(0)._4
        if (map.contains((e._1))) {
          val inner = map(e._1)
          inner.put(e._2, e._3)
          map.update(e._1, inner)
        } else {
          innerMap.put(e._2, e._3)
          map.put(e._1, innerMap)
        }
    }
    println("Map ", map.toString())
    (map, maxDate)
  }

  def countOrders(map: scala.collection.immutable.Map[Long, scala.collection.immutable.Map[Long, Int]]): (Int, Int, Int, Int, Int) = {
    var (cancel, ret, succ, inv, oth) = (0, 0, 0, 0, 0)
    map.keys.foreach{
      ordersId =>
        val itemMap = map(ordersId)
        if (itemMap.values.toSet.intersect(OrderStatus.SUCCESSFUL_ARRAY.toSet).size > 0)
          succ += 1
        else if (itemMap.values.toSet subsetOf (OrderStatus.CANCELLED_ARRAY.toSet))
          cancel += 1
        else if (itemMap.values.toSet subsetOf (OrderStatus.RETURN_ARRAY.toSet))
          ret += 1
        else if (itemMap.values.toSet subsetOf (scala.collection.immutable.Set(OrderStatus.INVALID)))
          inv += 1
        else
          oth += 1
    }
    (succ, cancel, ret, inv, oth)
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
        salesOrderJoined(SalesOrderVariables.CREATED_AT))

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

    orderValue
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

