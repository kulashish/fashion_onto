package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.{ Spark, Utils }
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.dataFeeds.DataFeedsModel
import com.jabong.dap.model.order.variables.SalesOrderItem
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable.HashMap

/**
 * Created by mubarak on 12/10/15.
 */

/*
  UID - CMR
  REVENUE_7 - sales_order_item
  REVENUE_30 = sales_order_item
  REVENUE_LIFETIME - sales_order_item
  GROSS_ORDERS - sales_order
  MAX_ORDER_BASKET_VALUE - sales_order_item
  MAX_ORDER_ITEM_VALUE - sales_order_item
  AVG_ORDER_VALUE - sales_order_item
  AVG_ORDER_ITEM_VALUE - sales_order_item
  FIRST_ORDER_DATE - sales_order
  COUNT_OF_RET_ORDERS - sales_order_item
  COUNT_OF_CNCLD_ORDERS - sales_order_item
  COUNT_OF_INVLD_ORDERS - sales_order_item
  FIRST_SHIPPING_CITY - sales_order, sales_order_address
  FIRST_SHIPPING_CITY_TIER - sales_order, sales_order_address
  LAST_SHIPPING_CITY  - sales_order, sales_order_address
  LAST_SHIPPING_CITY_TIER - sales_order, sales_order_address
  CATEGORY_PENETRATION - sales_order_item, ITR
  BRICK_PENETRATION - sales_order_item, ITR
  MIN_COUPON_VALUE_USED - sales_order_item, sales_order, sales_rule, sales_rule_set
  MAX_COUPON_VALUE_USED - sales_order_item, sales_order, sales_rule, sales_rule_set
  AVG_COUPON_VALUE_USED - sales_order_item, sales_order, sales_rule, sales_rule_set
  MIN_DISCOUNT_USED - sales_order_item, sales_order, sales_rule, sales_rule_set
  MAX_DISCOUNT_USED - sales_order_item, sales_order, sales_rule, sales_rule_set
  AVERAGE_DISCOUNT_USED - sales_order_item, sales_order, sales_rule, sales_rule_set
  */
object CustomerOrders extends DataFeedsModel {

  var incrDateLocal: String = null

  def canProcess(incrDate: String, saveMode: String): Boolean = {
    incrDateLocal = incrDate
    var savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.MAPS, DataSets.SALES_ITEM_INVALID_CANCEL, DataSets.FULL_MERGE_MODE, incrDate)
    var res = DataWriter.canWrite(saveMode, savePath)

    savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_ORDERS, DataSets.FULL_MERGE_MODE, incrDate)
    res = res || DataWriter.canWrite(saveMode, savePath)

    savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_ORDERS, DataSets.DAILY_MODE, incrDate)
    res = res || DataWriter.canWrite(saveMode, savePath)

    res
  }

  def readDF(incrDate: String, prevDate: String, paths: String): HashMap[String, DataFrame] = {

    val dfMap = new HashMap[String, DataFrame]()

    var mode: String = DataSets.FULL_MERGE_MODE
    if (null == paths) {
      mode = DataSets.DAILY_MODE
      val custOrdersPrevFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_ORDERS, DataSets.FULL_MERGE_MODE, prevDate)
      dfMap.put("custOrdersPrevFull", custOrdersPrevFull)
      val custOrdersStatusPrevMap = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.MAPS, DataSets.SALES_ITEM_INVALID_CANCEL, DataSets.FULL_MERGE_MODE, prevDate)
      dfMap.put("custOrdersStatusPrevMap", custOrdersStatusPrevMap)
    }

    val salesOrderFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.FULL_MERGE_MODE, incrDate)
    dfMap.put("salesOrderFull", salesOrderFull)
    val salesRuleFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_RULE, DataSets.FULL_MERGE_MODE, incrDate)
    dfMap.put("salesRuleFull", salesRuleFull)
    val dateDiffFormat = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_FORMAT)
    val salesRuleSetFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_RULE_SET, DataSets.FULL_FETCH_MODE, dateDiffFormat)
    dfMap.put("salesRuleSetFull", salesRuleSetFull)

    val salesOrderAddrFavIncr = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ORDER_ADDRESS, mode, incrDate)
    dfMap.put("salesOrderAddrFavIncr", salesOrderAddrFavIncr)
    val custTop5Incr = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_TOP5, mode, incrDate)
    dfMap.put("custTop5Incr", custTop5Incr)
    val salesRevenueFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.FULL_MERGE_MODE, incrDate)
    val salesOrderIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, mode, incrDate)
    val salesOrderItemIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ITEM, mode, incrDate)
    dfMap.put("salesOrderItemIncr", salesOrderItemIncr)

    var salesRevenueIncr = salesRevenueFull
    var salesOrderIncrFil = salesOrderIncr
    var salesOrderItemIncrFil = salesOrderItemIncr

    if (null == paths) {
      salesRevenueIncr = Utils.getOneDayData(salesRevenueFull, SalesOrderVariables.LAST_ORDER_DATE, incrDate, TimeConstants.DATE_FORMAT_FOLDER)
      salesOrderIncrFil = Utils.getOneDayData(salesOrderIncr, SalesOrderVariables.CREATED_AT, incrDate, TimeConstants.DATE_FORMAT_FOLDER)
      salesOrderItemIncrFil = Utils.getOneDayData(salesOrderItemIncr, SalesOrderVariables.CREATED_AT, incrDate, TimeConstants.DATE_FORMAT_FOLDER)
    }

    dfMap.put("salesRevenueIncr", salesRevenueIncr)
    dfMap.put("salesOrderIncrFil", salesOrderIncrFil)
    dfMap.put("salesOrderItemIncrFil", salesOrderItemIncrFil)

    val cmrFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, incrDate)
    dfMap.put("cmrFull", cmrFull)

    dfMap
  }

  def process(dfMap: HashMap[String, DataFrame]): HashMap[String, DataFrame] = {
    val salesOrderIncrFil = dfMap("salesOrderIncrFil")
    val salesOrderFull = dfMap("salesOrderFull")
    val salesOrderItemIncr = dfMap("salesOrderItemIncr")
    val salesOrderItemIncrFil = dfMap("salesOrderItemIncrFil")
    val salesRuleFull = dfMap("salesRuleFull")
    val salesRuleSetFull = dfMap("salesRuleSetFull")
    val salesOrderAddrFavIncr = dfMap("salesOrderAddrFavIncr")
    val custTop5Incr = dfMap("custTop5Incr")
    val salesRevenueIncr = dfMap("salesRevenueIncr")
    val custOrdersStatusPrevMap = dfMap.getOrElse("custOrdersStatusPrevMap", null)
    val custOrdersPrevFull = dfMap.getOrElse("custOrdersPrevFull", null)
    val cmrFull = dfMap("cmrFull")

    // println("salesOrderIncrFil " + salesOrderIncrFil.count())
    // println("salesRuleFull " + salesRuleFull.count())
    // println("salesRuleSetFull " + salesRuleSetFull.count())
    val salesDiscountIncr = SalesOrderItem.getCouponDisc(salesOrderIncrFil, salesRuleFull, salesRuleSetFull)
    // println("salesDiscountIncr " + salesDiscountIncr.count())
    // salesDiscountIncr.show(10)

    val dfWrite = new HashMap[String, DataFrame]()
    // println("salesOrderItemIncr " + salesOrderItemIncr.count())
    // println("salesOrderFull " + salesOrderFull.count())
    val (salesInvalidIncr, custOrdersStatusMap) = SalesOrderItem.getInvalidCancelOrders(salesOrderItemIncr, salesOrderFull, custOrdersStatusPrevMap, incrDateLocal)
    dfWrite.put("custOrdersStatusMap", custOrdersStatusMap)
    // println("custOrdersStatusMap ", custOrdersStatusMap.count())
    // custOrdersStatusMap.show(10)

    // println("custTop5Incr ", custTop5Incr.count())
    val salesCatBrick = custTop5Incr.select(
      custTop5Incr(SalesOrderVariables.FK_CUSTOMER),
      custTop5Incr("CAT_1") as SalesOrderVariables.CATEGORY_PENETRATION,
      custTop5Incr("BRICK_1") as SalesOrderVariables.BRICK_PENETRATION,
      custTop5Incr("BRAND_1") as SalesOrderItemVariables.FAV_BRAND
    )
    // println("salesCatBrick ", salesCatBrick.count())
    // salesCatBrick.show(10)

    val salesOrderNew = salesOrderIncrFil.na.fill(Map(
      SalesOrderVariables.GW_AMOUNT -> 0.0
    ))
    val salesOrderJoined = salesOrderNew
      .join(salesOrderItemIncrFil, salesOrderNew(SalesOrderVariables.ID_SALES_ORDER) === salesOrderItemIncrFil(SalesOrderVariables.FK_SALES_ORDER))
      .drop(salesOrderItemIncrFil(SalesOrderItemVariables.CREATED_AT))

    val salesOrderValueIncr = SalesOrderItem.getOrderValue(salesOrderJoined)
    // println("salesOrderValueIncr ", salesOrderValueIncr.count())
    // salesOrderValueIncr.printSchema()
    // salesOrderValueIncr.show(10)

    val custOrdersCalc = merger(salesRevenueIncr, salesDiscountIncr, salesInvalidIncr, salesCatBrick, salesOrderValueIncr, salesOrderAddrFavIncr)
    // println("custOrdersCalc ", custOrdersCalc.count())
    // custOrdersCalc.printSchema()
    // custOrdersCalc.show(10)

    val custOrderFull = joinCustOrder(custOrdersCalc, custOrdersPrevFull)
    // println("custOrderFull ", custOrderFull.count())
    // custOrderFull.printSchema()
    // custOrderFull.show(10)
    dfWrite.put("custOrderFull", custOrderFull)
    dfWrite.put("cmrFull", cmrFull)
    dfWrite
  }

  def write(dfWrite: HashMap[String, DataFrame], saveMode: String, incrDate: String) = {
    var savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.MAPS, DataSets.SALES_ITEM_INVALID_CANCEL, DataSets.FULL_MERGE_MODE, incrDate)
    if (DataWriter.canWrite(saveMode, savePath)) {
      DataWriter.writeParquet(dfWrite("custOrdersStatusMap"), savePath, saveMode)
    }
    val cmrFull = dfWrite("cmrFull")

    savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_ORDERS, DataSets.FULL_MERGE_MODE, incrDate)
    val custOrderFull = dfWrite("custOrderFull")
    if (DataWriter.canWrite(saveMode, savePath)) {
      DataWriter.writeParquet(custOrderFull, savePath, saveMode)
    }

    val custOrdersIncr = Utils.getOneDayData(custOrderFull, SalesOrderVariables.LAST_ORDER_DATE, incrDate, TimeConstants.DATE_FORMAT_FOLDER)

    savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_ORDERS, DataSets.DAILY_MODE, incrDate)
    if (DataWriter.canWrite(saveMode, savePath)) {
      DataWriter.writeParquet(custOrdersIncr, savePath, saveMode)
    }

    val finalCustOrder = custOrdersIncr.join(cmrFull, cmrFull(CustomerVariables.ID_CUSTOMER) === custOrdersIncr(SalesOrderVariables.FK_CUSTOMER), SQL.LEFT_OUTER)
      .select(
        cmrFull(ContactListMobileVars.UID),
        custOrdersIncr(SalesOrderItemVariables.REVENUE_7),
        custOrdersIncr(SalesOrderItemVariables.REVENUE_30),
        custOrdersIncr(SalesOrderItemVariables.REVENUE_LIFE),
        custOrdersIncr(SalesOrderItemVariables.GROSS_ORDERS),
        custOrdersIncr(SalesOrderVariables.MAX_ORDER_BASKET_VALUE),
        custOrdersIncr(SalesOrderVariables.MAX_ORDER_ITEM_VALUE),
        (custOrdersIncr(SalesOrderVariables.SUM_BASKET_VALUE) / custOrdersIncr(SalesOrderVariables.COUNT_BASKET_VALUE)) as SalesOrderVariables.AVG_ORDER_VALUE,
        (custOrdersIncr(SalesOrderVariables.SUM_BASKET_VALUE) / custOrdersIncr(SalesOrderVariables.ORDER_ITEM_COUNT)) as SalesOrderVariables.AVG_ORDER_ITEM_VALUE,
        custOrdersIncr(SalesOrderVariables.FIRST_ORDER_DATE),
        custOrdersIncr(SalesOrderItemVariables.COUNT_OF_RET_ORDERS),
        custOrdersIncr(SalesOrderItemVariables.COUNT_OF_CNCLD_ORDERS),
        custOrdersIncr(SalesOrderItemVariables.COUNT_OF_INVLD_ORDERS),
        custOrdersIncr(SalesAddressVariables.FIRST_SHIPPING_CITY),
        custOrdersIncr(SalesAddressVariables.FIRST_SHIPPING_CITY_TIER),
        custOrdersIncr(SalesAddressVariables.LAST_SHIPPING_CITY),
        custOrdersIncr(SalesAddressVariables.LAST_SHIPPING_CITY_TIER),
        custOrdersIncr(SalesOrderVariables.CATEGORY_PENETRATION),
        custOrdersIncr(SalesOrderVariables.BRICK_PENETRATION),
        custOrdersIncr(SalesRuleSetVariables.MIN_COUPON_VALUE_USED),
        custOrdersIncr(SalesRuleSetVariables.MAX_COUPON_VALUE_USED),
        (custOrdersIncr(SalesRuleSetVariables.COUPON_SUM) / custOrdersIncr(SalesRuleSetVariables.COUPON_COUNT)) as SalesRuleSetVariables.AVG_COUPON_VALUE_USED,
        custOrdersIncr(SalesRuleSetVariables.MIN_DISCOUNT_USED),
        custOrdersIncr(SalesRuleSetVariables.MAX_DISCOUNT_USED),
        (custOrdersIncr(SalesRuleSetVariables.DISCOUNT_SUM) / custOrdersIncr(SalesRuleSetVariables.DISCOUNT_COUNT)) as SalesRuleSetVariables.AVERAGE_DISCOUNT_USED
      ).na.fill("")
    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    DataWriter.writeCsv(finalCustOrder, DataSets.VARIABLES, DataSets.CUSTOMER_ORDERS, DataSets.DAILY_MODE, incrDate, fileDate + "_CUST_ORDERS", DataSets.IGNORE_SAVEMODE, "true", ";")

  }

  def joinCustOrder(incr: DataFrame, prevFull: DataFrame): DataFrame = {
    // This is being done as for decimal type columns the precision is becoming 20 and
    // while writing to parquet it is giving error
    val incrRdd = incr.select(CustomerVariables.FK_CUSTOMER,
      SalesOrderVariables.MAX_ORDER_BASKET_VALUE,
      SalesOrderVariables.MAX_ORDER_ITEM_VALUE,
      SalesOrderVariables.SUM_BASKET_VALUE,
      SalesOrderVariables.COUNT_BASKET_VALUE,
      SalesOrderVariables.ORDER_ITEM_COUNT,
      SalesOrderVariables.LAST_ORDER_DATE,
      SalesAddressVariables.LAST_SHIPPING_CITY,
      SalesAddressVariables.LAST_SHIPPING_CITY_TIER,
      SalesAddressVariables.FIRST_SHIPPING_CITY,
      SalesAddressVariables.FIRST_SHIPPING_CITY_TIER,
      SalesOrderItemVariables.COUNT_OF_INVLD_ORDERS,
      SalesOrderItemVariables.COUNT_OF_CNCLD_ORDERS,
      SalesOrderItemVariables.COUNT_OF_RET_ORDERS,
      SalesOrderItemVariables.SUCCESSFUL_ORDERS,
      SalesOrderItemVariables.GROSS_ORDERS,
      SalesOrderVariables.LAST_ORDER_UPDATED_AT,
      SalesOrderVariables.FIRST_ORDER_DATE,
      SalesRuleSetVariables.MIN_COUPON_VALUE_USED,
      SalesRuleSetVariables.MAX_COUPON_VALUE_USED,
      SalesRuleSetVariables.COUPON_SUM,
      SalesRuleSetVariables.COUPON_COUNT,
      SalesRuleSetVariables.MIN_DISCOUNT_USED,
      SalesRuleSetVariables.MAX_DISCOUNT_USED,
      SalesRuleSetVariables.DISCOUNT_SUM,
      SalesRuleSetVariables.DISCOUNT_COUNT,
      SalesOrderItemVariables.REVENUE_7,
      SalesOrderItemVariables.REVENUE_30,
      SalesOrderItemVariables.REVENUE_LIFE,
      SalesOrderItemVariables.ORDERS_COUNT_LIFE,
      SalesOrderVariables.CATEGORY_PENETRATION,
      SalesOrderVariables.BRICK_PENETRATION,
      SalesOrderItemVariables.FAV_BRAND,
      ContactListMobileVars.CITY,
      CustomerVariables.PHONE,
      CustomerVariables.FIRST_NAME,
      CustomerVariables.LAST_NAME,
      ContactListMobileVars.CITY_TIER,
      ContactListMobileVars.STATE_ZONE).rdd
    val custOrdersIncr = Spark.getSqlContext().createDataFrame(incrRdd, Schema.customerOrdersSchema)

    var res: DataFrame = custOrdersIncr
    if (null != prevFull) {
      val custOrdersPrevFull = Spark.getSqlContext().createDataFrame(prevFull.rdd, Schema.customerOrdersPrevSchema)
      val custOrdersFull = custOrdersIncr.join(custOrdersPrevFull, custOrdersIncr(SalesOrderVariables.FK_CUSTOMER) === custOrdersPrevFull(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
        .na.fill(scala.collection.immutable.Map(
          SalesOrderItemVariables.REVENUE_7 -> 0.0,
          SalesOrderItemVariables.REVENUE_30 -> 0.0,
          SalesOrderItemVariables.REVENUE_LIFE -> 0.0,
          SalesOrderItemVariables.ORDERS_COUNT_LIFE -> 0,
          SalesRuleSetVariables.MIN_COUPON_VALUE_USED -> 0.0,
          SalesRuleSetVariables.MAX_COUPON_VALUE_USED -> 0.0,
          SalesRuleSetVariables.MIN_DISCOUNT_USED -> 0.0,
          SalesRuleSetVariables.MAX_DISCOUNT_USED -> 0.0,
          SalesRuleSetVariables.COUPON_SUM -> 0.0,
          SalesRuleSetVariables.COUPON_COUNT -> 0,
          SalesRuleSetVariables.DISCOUNT_SUM -> 0.0,
          SalesRuleSetVariables.DISCOUNT_COUNT -> 0,
          SalesOrderItemVariables.COUNT_OF_INVLD_ORDERS -> 0,
          SalesOrderItemVariables.COUNT_OF_CNCLD_ORDERS -> 0,
          SalesOrderItemVariables.COUNT_OF_RET_ORDERS -> 0,
          SalesOrderItemVariables.SUCCESSFUL_ORDERS -> 0,
          SalesOrderItemVariables.GROSS_ORDERS -> 0,
          SalesOrderVariables.MAX_ORDER_BASKET_VALUE -> 0.0,
          SalesOrderVariables.MAX_ORDER_ITEM_VALUE -> 0.0,
          SalesOrderVariables.SUM_BASKET_VALUE -> 0.0,
          SalesOrderVariables.COUNT_BASKET_VALUE -> 0,
          SalesOrderVariables.ORDER_ITEM_COUNT -> 0
        ))
        .select(coalesce(custOrdersIncr(SalesOrderVariables.FK_CUSTOMER), custOrdersPrevFull(SalesOrderVariables.FK_CUSTOMER) + "_OLD") as SalesOrderVariables.FK_CUSTOMER,
          when(custOrdersIncr(SalesOrderVariables.MAX_ORDER_BASKET_VALUE) > custOrdersPrevFull(SalesOrderVariables.MAX_ORDER_BASKET_VALUE + "_OLD"), custOrdersIncr(SalesOrderVariables.MAX_ORDER_BASKET_VALUE)).otherwise(custOrdersPrevFull(SalesOrderVariables.MAX_ORDER_BASKET_VALUE + "_OLD")) as SalesOrderVariables.MAX_ORDER_BASKET_VALUE,
          when(custOrdersIncr(SalesOrderVariables.MAX_ORDER_ITEM_VALUE) > custOrdersPrevFull(SalesOrderVariables.MAX_ORDER_ITEM_VALUE + "_OLD"), custOrdersIncr(SalesOrderVariables.MAX_ORDER_ITEM_VALUE)).otherwise(custOrdersPrevFull(SalesOrderVariables.MAX_ORDER_ITEM_VALUE + "_OLD")) as SalesOrderVariables.MAX_ORDER_ITEM_VALUE,
          custOrdersIncr(SalesOrderVariables.SUM_BASKET_VALUE) + custOrdersPrevFull(SalesOrderVariables.SUM_BASKET_VALUE + "_OLD") as SalesOrderVariables.SUM_BASKET_VALUE,
          custOrdersIncr(SalesOrderVariables.COUNT_BASKET_VALUE) + custOrdersPrevFull(SalesOrderVariables.COUNT_BASKET_VALUE + "_OLD") as SalesOrderVariables.COUNT_BASKET_VALUE,
          custOrdersIncr(SalesOrderVariables.ORDER_ITEM_COUNT) + custOrdersPrevFull(SalesOrderVariables.ORDER_ITEM_COUNT + "_OLD") as SalesOrderVariables.ORDER_ITEM_COUNT,
          coalesce(custOrdersIncr(SalesOrderVariables.LAST_ORDER_DATE), custOrdersPrevFull(SalesOrderVariables.LAST_ORDER_DATE + "_OLD")) as SalesOrderVariables.LAST_ORDER_DATE,
          coalesce(custOrdersIncr(SalesAddressVariables.LAST_SHIPPING_CITY), custOrdersPrevFull(SalesAddressVariables.LAST_SHIPPING_CITY + "_OLD")) as SalesAddressVariables.LAST_SHIPPING_CITY,
          coalesce(custOrdersIncr(SalesAddressVariables.LAST_SHIPPING_CITY_TIER), custOrdersPrevFull(SalesAddressVariables.LAST_SHIPPING_CITY_TIER + "_OLD")) as SalesAddressVariables.LAST_SHIPPING_CITY_TIER,
          coalesce(custOrdersPrevFull(SalesAddressVariables.FIRST_SHIPPING_CITY + "_OLD"), custOrdersIncr(SalesAddressVariables.FIRST_SHIPPING_CITY)) as SalesAddressVariables.FIRST_SHIPPING_CITY,
          coalesce(custOrdersPrevFull(SalesAddressVariables.FIRST_SHIPPING_CITY_TIER + "_OLD"), custOrdersIncr(SalesAddressVariables.FIRST_SHIPPING_CITY_TIER)) as SalesAddressVariables.FIRST_SHIPPING_CITY_TIER,
          custOrdersPrevFull(SalesOrderItemVariables.COUNT_OF_INVLD_ORDERS + "_OLD") + custOrdersIncr(SalesOrderItemVariables.COUNT_OF_INVLD_ORDERS) as SalesOrderItemVariables.COUNT_OF_INVLD_ORDERS,
          custOrdersPrevFull(SalesOrderItemVariables.COUNT_OF_CNCLD_ORDERS + "_OLD") + custOrdersIncr(SalesOrderItemVariables.COUNT_OF_CNCLD_ORDERS) as SalesOrderItemVariables.COUNT_OF_CNCLD_ORDERS,
          custOrdersPrevFull(SalesOrderItemVariables.COUNT_OF_RET_ORDERS + "_OLD") + custOrdersIncr(SalesOrderItemVariables.COUNT_OF_RET_ORDERS) as SalesOrderItemVariables.COUNT_OF_RET_ORDERS,
          custOrdersPrevFull(SalesOrderItemVariables.SUCCESSFUL_ORDERS + "_OLD") + custOrdersIncr(SalesOrderItemVariables.SUCCESSFUL_ORDERS) as SalesOrderItemVariables.SUCCESSFUL_ORDERS,
          custOrdersPrevFull(SalesOrderItemVariables.GROSS_ORDERS + "_OLD") + custOrdersIncr(SalesOrderItemVariables.GROSS_ORDERS) as SalesOrderItemVariables.GROSS_ORDERS,
          coalesce(custOrdersIncr(SalesOrderVariables.LAST_ORDER_UPDATED_AT), custOrdersPrevFull(SalesOrderVariables.LAST_ORDER_UPDATED_AT + "_OLD")) as SalesOrderVariables.LAST_ORDER_UPDATED_AT,
          coalesce(custOrdersPrevFull(SalesOrderVariables.FIRST_ORDER_DATE + "_OLD"), custOrdersIncr(SalesOrderVariables.FIRST_ORDER_DATE)) as SalesOrderVariables.FIRST_ORDER_DATE,
          when(custOrdersIncr(SalesRuleSetVariables.MIN_COUPON_VALUE_USED) < custOrdersPrevFull(SalesRuleSetVariables.MIN_COUPON_VALUE_USED + "_OLD"), custOrdersIncr(SalesRuleSetVariables.MIN_COUPON_VALUE_USED)).otherwise(custOrdersPrevFull(SalesRuleSetVariables.MIN_COUPON_VALUE_USED + "_OLD")) as SalesRuleSetVariables.MIN_COUPON_VALUE_USED,
          when(custOrdersIncr(SalesRuleSetVariables.MAX_COUPON_VALUE_USED) > custOrdersPrevFull(SalesRuleSetVariables.MAX_COUPON_VALUE_USED + "_OLD"), custOrdersIncr(SalesRuleSetVariables.MAX_COUPON_VALUE_USED)).otherwise(custOrdersPrevFull(SalesRuleSetVariables.MAX_COUPON_VALUE_USED + "_OLD")) as SalesRuleSetVariables.MAX_COUPON_VALUE_USED,
          custOrdersIncr(SalesRuleSetVariables.COUPON_SUM) + custOrdersPrevFull(SalesRuleSetVariables.COUPON_SUM + "_OLD") as SalesRuleSetVariables.COUPON_SUM,
          custOrdersIncr(SalesRuleSetVariables.COUPON_COUNT) + custOrdersPrevFull(SalesRuleSetVariables.COUPON_COUNT + "_OLD") as SalesRuleSetVariables.COUPON_COUNT,
          when(custOrdersIncr(SalesRuleSetVariables.MIN_DISCOUNT_USED) < custOrdersPrevFull(SalesRuleSetVariables.MIN_DISCOUNT_USED + "_OLD"), custOrdersIncr(SalesRuleSetVariables.MIN_DISCOUNT_USED)).otherwise(custOrdersPrevFull(SalesRuleSetVariables.MIN_DISCOUNT_USED + "_OLD")) as SalesRuleSetVariables.MIN_DISCOUNT_USED,
          when(custOrdersIncr(SalesRuleSetVariables.MAX_DISCOUNT_USED) < custOrdersPrevFull(SalesRuleSetVariables.MAX_DISCOUNT_USED + "_OLD"), custOrdersIncr(SalesRuleSetVariables.MAX_DISCOUNT_USED)).otherwise(custOrdersPrevFull(SalesRuleSetVariables.MAX_DISCOUNT_USED + "_OLD")) as SalesRuleSetVariables.MAX_DISCOUNT_USED,
          custOrdersIncr(SalesRuleSetVariables.DISCOUNT_SUM) + custOrdersPrevFull(SalesRuleSetVariables.DISCOUNT_SUM + "_OLD") as SalesRuleSetVariables.DISCOUNT_SUM,
          custOrdersIncr(SalesRuleSetVariables.DISCOUNT_COUNT) + custOrdersPrevFull(SalesRuleSetVariables.DISCOUNT_COUNT + "_OLD") as SalesRuleSetVariables.DISCOUNT_COUNT,
          coalesce(custOrdersIncr(SalesOrderItemVariables.REVENUE_7), custOrdersPrevFull(SalesOrderItemVariables.REVENUE_7 + "_OLD")) as SalesOrderItemVariables.REVENUE_7,
          coalesce(custOrdersIncr(SalesOrderItemVariables.REVENUE_30), custOrdersPrevFull(SalesOrderItemVariables.REVENUE_30 + "_OLD")) as SalesOrderItemVariables.REVENUE_30,
          coalesce(custOrdersIncr(SalesOrderItemVariables.REVENUE_LIFE), custOrdersPrevFull(SalesOrderItemVariables.REVENUE_LIFE + "_OLD")) as SalesOrderItemVariables.REVENUE_LIFE,
          coalesce(custOrdersIncr(SalesOrderItemVariables.ORDERS_COUNT_LIFE), custOrdersPrevFull(SalesOrderItemVariables.ORDERS_COUNT_LIFE + "_OLD")) as SalesOrderItemVariables.ORDERS_COUNT_LIFE,
          coalesce(custOrdersIncr(SalesOrderVariables.CATEGORY_PENETRATION), custOrdersPrevFull(SalesOrderVariables.CATEGORY_PENETRATION + "_OLD")) as SalesOrderVariables.CATEGORY_PENETRATION,
          coalesce(custOrdersIncr(SalesOrderVariables.BRICK_PENETRATION), custOrdersPrevFull(SalesOrderVariables.BRICK_PENETRATION + "_OLD")) as SalesOrderVariables.BRICK_PENETRATION,
          coalesce(custOrdersIncr(SalesOrderItemVariables.FAV_BRAND), custOrdersPrevFull(SalesOrderItemVariables.FAV_BRAND + "_OLD")) as SalesOrderItemVariables.FAV_BRAND,
          coalesce(custOrdersIncr(ContactListMobileVars.CITY), custOrdersPrevFull(ContactListMobileVars.CITY + "_OLD")) as ContactListMobileVars.CITY,
          coalesce(custOrdersIncr(CustomerVariables.PHONE), custOrdersPrevFull(CustomerVariables.PHONE + "_OLD")) as CustomerVariables.PHONE,
          coalesce(custOrdersIncr(CustomerVariables.FIRST_NAME), custOrdersPrevFull(CustomerVariables.FIRST_NAME + "_OLD")) as CustomerVariables.FIRST_NAME,
          coalesce(custOrdersIncr(CustomerVariables.LAST_NAME), custOrdersPrevFull(CustomerVariables.LAST_NAME + "_OLD")) as CustomerVariables.LAST_NAME,
          coalesce(custOrdersIncr(ContactListMobileVars.CITY_TIER), custOrdersPrevFull(ContactListMobileVars.CITY_TIER + "_OLD")) as ContactListMobileVars.CITY_TIER,
          coalesce(custOrdersIncr(ContactListMobileVars.STATE_ZONE), custOrdersPrevFull(ContactListMobileVars.STATE_ZONE + "_OLD")) as ContactListMobileVars.STATE_ZONE
        )
      // This is being done as for decimal type columns the precision is becoming 20 and
      // while writing to parquet it is giving error
      res = Spark.getSqlContext().createDataFrame(custOrdersFull.rdd, Schema.customerOrdersSchema)
    }
    res
  }

  def merger(salesRevenueVariables: DataFrame, salesDiscount: DataFrame, salesInvalid: DataFrame, salesCatBrick: DataFrame, salesOrderValue: DataFrame, salesOrderAddrFavIncr: DataFrame): DataFrame = {
    if (null == salesRevenueVariables || null == salesDiscount || null == salesInvalid || null == salesCatBrick || null == salesOrderValue || null == salesOrderAddrFavIncr) {
      println("Any of the Input dataFrames is null !!!")
      return null
    }

    val revJoined = salesRevenueVariables.join(salesDiscount, salesDiscount(SalesOrderVariables.FK_CUSTOMER) === salesRevenueVariables(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
      .select(coalesce(salesDiscount(SalesOrderVariables.FK_CUSTOMER), salesRevenueVariables(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
        salesRevenueVariables(SalesOrderItemVariables.REVENUE_7),
        salesRevenueVariables(SalesOrderItemVariables.REVENUE_30),
        salesRevenueVariables(SalesOrderItemVariables.REVENUE_LIFE),
        salesRevenueVariables(SalesOrderItemVariables.ORDERS_COUNT_LIFE),
        salesRevenueVariables(SalesOrderVariables.LAST_ORDER_DATE),
        salesDiscount(SalesRuleSetVariables.MIN_COUPON_VALUE_USED),
        salesDiscount(SalesRuleSetVariables.MAX_COUPON_VALUE_USED),
        salesDiscount(SalesRuleSetVariables.COUPON_SUM),
        salesDiscount(SalesRuleSetVariables.COUPON_COUNT),
        salesDiscount(SalesRuleSetVariables.MIN_DISCOUNT_USED),
        salesDiscount(SalesRuleSetVariables.MAX_DISCOUNT_USED),
        salesDiscount(SalesRuleSetVariables.DISCOUNT_SUM),
        salesDiscount(SalesRuleSetVariables.DISCOUNT_COUNT))

    val invalidJoined = revJoined.join(salesInvalid, salesInvalid(SalesOrderVariables.FK_CUSTOMER) === revJoined(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
      .select(coalesce(revJoined(SalesOrderVariables.FK_CUSTOMER), salesInvalid(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
        revJoined(SalesOrderItemVariables.REVENUE_7),
        revJoined(SalesOrderItemVariables.REVENUE_30),
        revJoined(SalesOrderItemVariables.REVENUE_LIFE),
        revJoined(SalesOrderItemVariables.ORDERS_COUNT_LIFE),
        revJoined(SalesOrderVariables.LAST_ORDER_DATE),
        revJoined(SalesRuleSetVariables.MIN_COUPON_VALUE_USED),
        revJoined(SalesRuleSetVariables.MAX_COUPON_VALUE_USED),
        revJoined(SalesRuleSetVariables.MIN_DISCOUNT_USED),
        revJoined(SalesRuleSetVariables.MAX_DISCOUNT_USED),
        revJoined(SalesRuleSetVariables.COUPON_SUM),
        revJoined(SalesRuleSetVariables.COUPON_COUNT),
        revJoined(SalesRuleSetVariables.DISCOUNT_SUM),
        revJoined(SalesRuleSetVariables.DISCOUNT_COUNT),
        salesInvalid(SalesOrderItemVariables.COUNT_OF_INVLD_ORDERS),
        salesInvalid(SalesOrderItemVariables.COUNT_OF_CNCLD_ORDERS),
        salesInvalid(SalesOrderItemVariables.COUNT_OF_RET_ORDERS),
        salesInvalid(SalesOrderItemVariables.SUCCESSFUL_ORDERS),
        salesInvalid(SalesOrderItemVariables.COUNT_OF_INVLD_ORDERS) + salesInvalid(SalesOrderItemVariables.COUNT_OF_CNCLD_ORDERS) + salesInvalid(SalesOrderItemVariables.COUNT_OF_RET_ORDERS) + salesInvalid(SalesOrderItemVariables.SUCCESSFUL_ORDERS) + salesInvalid("others") as SalesOrderItemVariables.GROSS_ORDERS,
        salesInvalid(SalesOrderVariables.LAST_ORDER_UPDATED_AT),
        salesInvalid(SalesOrderVariables.FIRST_ORDER_DATE)
      )

    val catBrickJoined = invalidJoined.join(salesCatBrick, salesCatBrick(SalesOrderVariables.FK_CUSTOMER) === invalidJoined(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
      .select(coalesce(invalidJoined(SalesOrderVariables.FK_CUSTOMER), salesCatBrick(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
        invalidJoined(SalesOrderItemVariables.REVENUE_7),
        invalidJoined(SalesOrderItemVariables.REVENUE_30),
        invalidJoined(SalesOrderItemVariables.REVENUE_LIFE),
        invalidJoined(SalesOrderItemVariables.ORDERS_COUNT_LIFE),
        invalidJoined(SalesOrderVariables.LAST_ORDER_DATE),
        invalidJoined(SalesRuleSetVariables.MIN_COUPON_VALUE_USED),
        invalidJoined(SalesRuleSetVariables.MAX_COUPON_VALUE_USED),
        invalidJoined(SalesRuleSetVariables.MIN_DISCOUNT_USED),
        invalidJoined(SalesRuleSetVariables.MAX_DISCOUNT_USED),
        invalidJoined(SalesOrderItemVariables.COUNT_OF_INVLD_ORDERS),
        invalidJoined(SalesOrderItemVariables.COUNT_OF_CNCLD_ORDERS),
        invalidJoined(SalesOrderItemVariables.COUNT_OF_RET_ORDERS),
        invalidJoined(SalesOrderItemVariables.SUCCESSFUL_ORDERS),
        invalidJoined(SalesOrderItemVariables.GROSS_ORDERS),
        invalidJoined(SalesOrderVariables.LAST_ORDER_UPDATED_AT),
        invalidJoined(SalesOrderVariables.FIRST_ORDER_DATE),
        invalidJoined(SalesRuleSetVariables.COUPON_SUM),
        invalidJoined(SalesRuleSetVariables.COUPON_COUNT),
        invalidJoined(SalesRuleSetVariables.DISCOUNT_SUM),
        invalidJoined(SalesRuleSetVariables.DISCOUNT_COUNT),
        salesCatBrick(SalesOrderVariables.CATEGORY_PENETRATION),
        salesCatBrick(SalesOrderVariables.BRICK_PENETRATION),
        salesCatBrick(SalesOrderItemVariables.FAV_BRAND)
      )
    val salesValueJoined = salesOrderValue.join(catBrickJoined, catBrickJoined(SalesOrderVariables.FK_CUSTOMER) === salesOrderValue(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
      .select(coalesce(catBrickJoined(SalesOrderVariables.FK_CUSTOMER), salesOrderValue(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
        catBrickJoined(SalesOrderItemVariables.REVENUE_7),
        catBrickJoined(SalesOrderItemVariables.REVENUE_30),
        catBrickJoined(SalesOrderItemVariables.REVENUE_LIFE),
        catBrickJoined(SalesOrderItemVariables.ORDERS_COUNT_LIFE),
        catBrickJoined(SalesOrderVariables.LAST_ORDER_DATE),
        catBrickJoined(SalesRuleSetVariables.MIN_COUPON_VALUE_USED),
        catBrickJoined(SalesRuleSetVariables.MAX_COUPON_VALUE_USED),
        catBrickJoined(SalesRuleSetVariables.MIN_DISCOUNT_USED),
        catBrickJoined(SalesRuleSetVariables.MAX_DISCOUNT_USED),
        catBrickJoined(SalesRuleSetVariables.COUPON_SUM),
        catBrickJoined(SalesRuleSetVariables.COUPON_COUNT),
        catBrickJoined(SalesRuleSetVariables.DISCOUNT_SUM),
        catBrickJoined(SalesRuleSetVariables.DISCOUNT_COUNT),
        catBrickJoined(SalesOrderItemVariables.COUNT_OF_INVLD_ORDERS),
        catBrickJoined(SalesOrderItemVariables.COUNT_OF_CNCLD_ORDERS),
        catBrickJoined(SalesOrderItemVariables.COUNT_OF_RET_ORDERS),
        catBrickJoined(SalesOrderItemVariables.SUCCESSFUL_ORDERS),
        catBrickJoined(SalesOrderItemVariables.GROSS_ORDERS),
        catBrickJoined(SalesOrderVariables.LAST_ORDER_UPDATED_AT),
        catBrickJoined(SalesOrderVariables.FIRST_ORDER_DATE),
        catBrickJoined(SalesOrderVariables.CATEGORY_PENETRATION),
        catBrickJoined(SalesOrderVariables.BRICK_PENETRATION),
        catBrickJoined(SalesOrderItemVariables.FAV_BRAND),
        salesOrderValue(SalesOrderVariables.MAX_ORDER_BASKET_VALUE),
        salesOrderValue(SalesOrderVariables.MAX_ORDER_ITEM_VALUE),
        salesOrderValue(SalesOrderVariables.SUM_BASKET_VALUE),
        salesOrderValue(SalesOrderVariables.COUNT_BASKET_VALUE),
        salesOrderValue(SalesOrderVariables.ORDER_ITEM_COUNT)
      )

    val res = salesValueJoined.join(salesOrderAddrFavIncr, salesValueJoined(SalesOrderVariables.FK_CUSTOMER) === salesOrderAddrFavIncr(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
      .select(coalesce(salesValueJoined(SalesOrderVariables.FK_CUSTOMER), salesOrderAddrFavIncr(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
        salesValueJoined(SalesOrderItemVariables.REVENUE_7),
        salesValueJoined(SalesOrderItemVariables.REVENUE_30),
        salesValueJoined(SalesOrderItemVariables.REVENUE_LIFE),
        salesValueJoined(SalesOrderItemVariables.ORDERS_COUNT_LIFE),
        salesValueJoined(SalesOrderVariables.LAST_ORDER_DATE),
        salesValueJoined(SalesRuleSetVariables.MIN_COUPON_VALUE_USED),
        salesValueJoined(SalesRuleSetVariables.MAX_COUPON_VALUE_USED),
        salesValueJoined(SalesRuleSetVariables.MIN_DISCOUNT_USED),
        salesValueJoined(SalesRuleSetVariables.MAX_DISCOUNT_USED),
        salesValueJoined(SalesRuleSetVariables.COUPON_SUM),
        salesValueJoined(SalesRuleSetVariables.COUPON_COUNT),
        salesValueJoined(SalesRuleSetVariables.DISCOUNT_SUM),
        salesValueJoined(SalesRuleSetVariables.DISCOUNT_COUNT),
        salesValueJoined(SalesOrderItemVariables.COUNT_OF_INVLD_ORDERS),
        salesValueJoined(SalesOrderItemVariables.COUNT_OF_CNCLD_ORDERS),
        salesValueJoined(SalesOrderItemVariables.COUNT_OF_RET_ORDERS),
        salesValueJoined(SalesOrderItemVariables.SUCCESSFUL_ORDERS),
        salesValueJoined(SalesOrderItemVariables.GROSS_ORDERS),
        salesValueJoined(SalesOrderVariables.LAST_ORDER_UPDATED_AT),
        salesValueJoined(SalesOrderVariables.FIRST_ORDER_DATE),
        salesValueJoined(SalesOrderVariables.CATEGORY_PENETRATION),
        salesValueJoined(SalesOrderVariables.BRICK_PENETRATION),
        salesValueJoined(SalesOrderItemVariables.FAV_BRAND),
        salesValueJoined(SalesOrderVariables.MAX_ORDER_BASKET_VALUE),
        salesValueJoined(SalesOrderVariables.MAX_ORDER_ITEM_VALUE),
        salesValueJoined(SalesOrderVariables.SUM_BASKET_VALUE),
        salesValueJoined(SalesOrderVariables.COUNT_BASKET_VALUE),
        salesValueJoined(SalesOrderVariables.ORDER_ITEM_COUNT),
        salesOrderAddrFavIncr(SalesAddressVariables.FIRST_SHIPPING_CITY),
        salesOrderAddrFavIncr(SalesAddressVariables.FIRST_SHIPPING_CITY_TIER),
        salesOrderAddrFavIncr(SalesAddressVariables.LAST_SHIPPING_CITY),
        salesOrderAddrFavIncr(SalesAddressVariables.LAST_SHIPPING_CITY_TIER),
        salesOrderAddrFavIncr(ContactListMobileVars.CITY),
        salesOrderAddrFavIncr(CustomerVariables.PHONE),
        salesOrderAddrFavIncr(CustomerVariables.FIRST_NAME),
        salesOrderAddrFavIncr(CustomerVariables.LAST_NAME),
        salesOrderAddrFavIncr(ContactListMobileVars.CITY_TIER),
        salesOrderAddrFavIncr(ContactListMobileVars.STATE_ZONE)
      )
    // println("before filling zeros: ")
    // res.printSchema()
    res.na.fill(scala.collection.immutable.Map(
      SalesOrderItemVariables.REVENUE_7 -> 0.0,
      SalesOrderItemVariables.REVENUE_30 -> 0.0,
      SalesOrderItemVariables.REVENUE_LIFE -> 0.0,
      SalesOrderItemVariables.ORDERS_COUNT_LIFE -> 0,
      SalesRuleSetVariables.MIN_COUPON_VALUE_USED -> 0.0,
      SalesRuleSetVariables.MAX_COUPON_VALUE_USED -> 0.0,
      SalesRuleSetVariables.MIN_DISCOUNT_USED -> 0.0,
      SalesRuleSetVariables.MAX_DISCOUNT_USED -> 0.0,
      SalesRuleSetVariables.COUPON_SUM -> 0.0,
      SalesRuleSetVariables.COUPON_COUNT -> 0,
      SalesRuleSetVariables.DISCOUNT_SUM -> 0.0,
      SalesRuleSetVariables.DISCOUNT_COUNT -> 0,
      SalesOrderItemVariables.COUNT_OF_INVLD_ORDERS -> 0,
      SalesOrderItemVariables.COUNT_OF_CNCLD_ORDERS -> 0,
      SalesOrderItemVariables.COUNT_OF_RET_ORDERS -> 0,
      SalesOrderItemVariables.SUCCESSFUL_ORDERS -> 0,
      SalesOrderItemVariables.GROSS_ORDERS -> 0,
      SalesOrderVariables.MAX_ORDER_BASKET_VALUE -> 0.0,
      SalesOrderVariables.MAX_ORDER_ITEM_VALUE -> 0.0,
      SalesOrderVariables.SUM_BASKET_VALUE -> 0.0,
      SalesOrderVariables.COUNT_BASKET_VALUE -> 0,
      SalesOrderVariables.ORDER_ITEM_COUNT -> 0
    ))
  }
}

