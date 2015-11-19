package com.jabong.dap.model.order.variables

import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.time.{ TimeUtils, TimeConstants }
import com.jabong.dap.common.{ Utils, Spark }
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.variables.{ SalesOrderItemVariables, SalesOrderVariables }
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.dataFeeds.DataFeedsModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.HashMap

/**
 * Created by pooja on 19/11/15.
 */
object SalesItemRevenue extends DataFeedsModel {

  def canProcess(incrDate: String, saveMode: String): Boolean = {

    val savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.FULL_MERGE_MODE, incrDate)
    val savePathIncr = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.DAILY_MODE, incrDate)
    (DataWriter.canWrite(saveMode, savePath) || DataWriter.canWrite(saveMode, savePathIncr))

  }

  def readDF(incrDate: String, prevDate: String, paths: String): HashMap[String, DataFrame] = {

    val dfMap = new HashMap[String, DataFrame]()

    // This has to fail if we don't get the prev file.
    // for first time use one time script which will run for 90 days.
    val salesRevenuePrevFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.FULL_MERGE_MODE, prevDate)
    dfMap.put ("salesRevenuePrevFull", salesRevenuePrevFull)

    val salesOrderIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.DAILY_MODE, incrDate)
    val salesOrderIncrFil = Utils.getOneDayData(salesOrderIncr, SalesOrderVariables.CREATED_AT, incrDate, TimeConstants.DATE_FORMAT_FOLDER)
    dfMap.put("salesOrderIncrFil", salesOrderIncrFil)
    val salesOrderItemIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ITEM, DataSets.DAILY_MODE, incrDate)
    val salesOrderItemIncrFil = Utils.getOneDayData(salesOrderItemIncr, SalesOrderVariables.CREATED_AT, incrDate, TimeConstants.DATE_FORMAT_FOLDER)
    dfMap.put("salesOrderItemIncrFil", salesOrderItemIncrFil)

    val before7 = TimeUtils.getDateAfterNDays(-7, TimeConstants.DATE_FORMAT_FOLDER, prevDate)
    val salesRevenue7 = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.DAILY_MODE, before7)
    dfMap.put("salesRevenue7", salesRevenue7)
    val before30 = TimeUtils.getDateAfterNDays(-30, TimeConstants.DATE_FORMAT_FOLDER, prevDate)
    val salesRevenue30 = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.DAILY_MODE, before30)
    dfMap.put("salesRevenue30", salesRevenue30)
    val before90 = TimeUtils.getDateAfterNDays(-90, TimeConstants.DATE_FORMAT_FOLDER, prevDate)
    val salesRevenue90 = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.DAILY_MODE, before90)
    dfMap.put("salesRevenue90", salesRevenue90)

    dfMap
  }

  /**
   * Creates order_count(app,web,mweb) and Revenue(app,web,mweb)
   * @param dfMap sales_order, sales_order_item tables joined data
   * @return Dataframe with the latest values for orders_count and ravenue for each customer
   */
  def process(dfMap: HashMap[String, DataFrame]): HashMap[String, DataFrame] = {
    val salesRevenuePrevFull = dfMap("salesRevenuePrevFull")
    val salesOrderIncrFil = dfMap("salesOrderIncrFil")
    val salesOrderItemIncrFil = dfMap("salesOrderItemIncrFil")
    val salesRevenue7 = dfMap("salesRevenue7")
    val salesRevenue30 = dfMap("salesRevenue30")
    val salesRevenue90 = dfMap("salesRevenue90")

    val salesOrderNew = salesOrderIncrFil.na.fill(Map(
      SalesOrderVariables.GW_AMOUNT -> 0.0
    ))
    val salesOrderJoined = salesOrderNew
      .join(salesOrderItemIncrFil, salesOrderNew(SalesOrderVariables.ID_SALES_ORDER) === salesOrderItemIncrFil(SalesOrderVariables.FK_SALES_ORDER))
      .drop(salesOrderItemIncrFil(SalesOrderItemVariables.CREATED_AT))

    val (salesRevenueOrdersIncr, salesRevenueFull) = getRevenueOrdersCount(salesOrderJoined, salesRevenuePrevFull, salesRevenue7, salesRevenue30, salesRevenue90)

    val dfWrite = new HashMap[String, DataFrame]()
    dfWrite.put("salesRevenueFull", salesRevenueFull)
    dfWrite.put("salesRevenueOrdersIncr", salesRevenueOrdersIncr)

    dfWrite
  }

  def write(dfWrite: HashMap[String, DataFrame], saveMode: String, incrDate: String) = {
    var savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.FULL_MERGE_MODE, incrDate)
    if (DataWriter.canWrite(saveMode, savePath)) {
      DataWriter.writeParquet(dfWrite("salesRevenueFull"), savePath, saveMode)
    }

    var savePathIncr = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.DAILY_MODE, incrDate)
    if (DataWriter.canWrite(saveMode, savePathIncr)) {
      DataWriter.writeParquet(dfWrite("salesRevenueOrdersIncr"), savePathIncr, saveMode)
    }
  }

  def getRevenueOrdersCount(salesOrderJoined: DataFrame, salesRevenuePrevFull: DataFrame, salesRevenue7: DataFrame, salesRevenue30: DataFrame, salesRevenue90: DataFrame): (DataFrame, DataFrame) = {
    val salesJoinedDF = salesOrderJoined
      .select(
        salesOrderJoined(SalesOrderVariables.FK_CUSTOMER),
        salesOrderJoined(SalesOrderVariables.COD_CHARGE),
        salesOrderJoined(SalesOrderVariables.GW_AMOUNT),
        salesOrderJoined(SalesOrderVariables.SHIPPING_AMOUNT),
        salesOrderJoined(SalesOrderVariables.ID_SALES_ORDER),
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
    val otherOrders = salesJoinedDF.filter(salesJoinedDF(SalesOrderVariables.DOMAIN).isNull
      || !salesJoinedDF(SalesOrderVariables.DOMAIN).startsWith("w")
      || !salesJoinedDF(SalesOrderVariables.DOMAIN).startsWith("m")
      || !salesJoinedDF(SalesOrderVariables.DOMAIN).startsWith("ios")
      || !salesJoinedDF(SalesOrderVariables.DOMAIN).startsWith("android")
      || !salesJoinedDF(SalesOrderVariables.DOMAIN).startsWith("windows"))
    val app = getRevenueOrders(appOrders, "_app")
    val web = getRevenueOrders(webOrders, "_web")
    val mWeb = getRevenueOrders(mWebOrders, "_mweb")
    val others = getRevenueOrders(otherOrders, "_mweb")
    val salesRevenueOrdersIncr = joinDataFrames(app, web, mWeb)

    val mergedData = merge(salesRevenueOrdersIncr, salesRevenuePrevFull)

    val res7 = getRevenueDays(salesRevenue7, mergedData, 7, 30, 90)

    val res30 = getRevenueDays(salesRevenue30, res7, 30, 7, 90)

    val salesRevenueFull = getRevenueDays(salesRevenue90, res30, 90, 7, 30)

    (salesRevenueOrdersIncr, salesRevenueFull)
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

}
