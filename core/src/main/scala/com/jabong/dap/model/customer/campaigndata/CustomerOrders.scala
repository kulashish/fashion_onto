package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.Utils
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.dataFeeds.DataFeedsModel
import com.jabong.dap.model.order.variables.{ SalesOrderAddress, SalesOrderItem }
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
    val res = true
    res
  }

  def process(dfMap: HashMap[String, DataFrame]): HashMap[String, DataFrame] = {
    val salesOrderIncr = dfMap("salesOrderIncr")
    val salesOrderIncrFil = dfMap("salesOrderIncrFil")
    val salesOrderFull = dfMap("salesOrderFull")
    val salesOrderItemIncr = dfMap("salesOrderItemIncr")
    val salesOrderItemIncrFil = dfMap("salesOrderItemIncrFil")
    val salesRuleFull = dfMap("salesRuleFull")
    val salesRuleSetFull = dfMap("salesRuleSetFull")
    val salesAddressFull = dfMap("salesAddressFull")
    val custTop5Incr = dfMap("custTop5Incr")
    val cityZone = dfMap("cityZone")
    val salesRevenuePrevFull = dfMap("salesRevenuePrevFull")
    val salesRevenue7 = dfMap("salesRevenue7")
    val salesRevenue30 = dfMap("salesRevenue30")
    val salesRevenue90 = dfMap("salesRevenue90")
    val salesRuleCalc = dfMap("salesRuleCalc")
    val salesItemInvalidCalc = dfMap("salesItemInvalidCalc")
    val salesCatBrickCalc = dfMap("salesCatBrickCalc")
    val salesOrderValueCalc = dfMap("salesOrderValueCalc")
    val salesAddressCalc = dfMap("salesAddressCalc")
    val custOrdersStatusMap = dfMap("custOrdersStatusMap")

    val salesOrderNew = salesOrderIncrFil.na.fill(Map(
      SalesOrderVariables.GW_AMOUNT -> 0.0
    ))
    val saleOrderJoined = salesOrderNew.join(salesOrderItemIncrFil, salesOrderNew(SalesOrderVariables.ID_SALES_ORDER) === salesOrderItemIncrFil(SalesOrderVariables.FK_SALES_ORDER)).drop(salesOrderItemIncrFil(SalesOrderItemVariables.CREATED_AT))

    val (salesVariablesIncr, salesVariablesFull) = SalesOrderItem.getRevenueOrdersCount(saleOrderJoined, salesRevenuePrevFull, salesRevenue7, salesRevenue30, salesRevenue90)

    var dfWrite = new HashMap[String, DataFrame]()
    dfWrite.put("salesVariablesFull", salesVariablesFull)
    dfWrite.put("salesVariablesIncr", salesVariablesIncr)

    val salesRevenueIncr = Utils.getOneDayData(salesVariablesFull, SalesOrderVariables.CREATED_AT, incrDateLocal, TimeConstants.DATE_FORMAT_FOLDER)

    val salesDiscountIncr = SalesOrderItem.getCouponDisc(salesOrderIncr, salesRuleFull, salesRuleSetFull, salesRuleCalc)

    val (salesInvalidIncr, orderStatusmap) = SalesOrderItem.getInvalidCancelOrders(salesOrderItemIncr, salesOrderFull, custOrdersStatusMap, incrDateLocal)
    dfWrite.put("orderStatusmap", salesInvalidIncr)

    val salesCatBrick = custTop5Incr.select(
      custTop5Incr("CAT_1") as SalesOrderVariables.CATEGORY_PENETRATION,
      custTop5Incr("BRICK_1") as SalesOrderVariables.BRICK_PENETRATION,
      custTop5Incr("BRAND_1") as SalesOrderItemVariables.FAV_BRAND
    )

    val salesOrderValueIncr = SalesOrderItem.getOrderValue(saleOrderJoined)

    val salesAddressFirstIncr = SalesOrderAddress.getFirstShippingCity(salesOrderIncr, salesAddressFull, salesAddressCalc, cityZone)
    //val salesRevIncr = Utils.getOneDayData(salesVariablesFull, SalesOrderVariables.LAST_ORDER_DATE, incrDate, TimeConstants.DATE_FORMAT_FOLDER)
    val custOrdersCalc = merger(salesRevenueIncr, salesDiscountIncr, salesInvalidIncr, salesCatBrick, salesOrderValueIncr, salesAddressFirstIncr)

    val custOrderFull = joinCustOrder(custOrdersCalc, custOrdersStatusMap)
    dfWrite.put("custOrderFull", custOrderFull)

    dfWrite
  }

  def write(dfWrite: HashMap[String, DataFrame], saveMode: String, incrDate: String) = {
    var savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(dfWrite("salesVariablesFull"), savePath, saveMode)

    var savePathIncr = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.DAILY_MODE, incrDate)
    DataWriter.writeParquet(dfWrite("salesVariablesIncr"), savePathIncr, saveMode)


    savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_INVALID_CANCEL, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(dfWrite("orderStatusmap"), savePath, saveMode)

    savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_ORDERS, DataSets.FULL_MERGE_MODE, incrDate)
    val custOrderFull = dfWrite("custOrderFull")
    DataWriter.writeParquet(custOrderFull, savePath, saveMode)

    val custOrdersIncr = Utils.getOneDayData(custOrderFull, SalesOrderVariables.LAST_ORDER_DATE, incrDate, TimeConstants.DATE_FORMAT_FOLDER)

    savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_ORDERS, DataSets.DAILY_MODE, incrDate)
    DataWriter.writeParquet(custOrdersIncr, savePath, saveMode)

    val custOrdersCsv = custOrdersIncr
      .withColumn(SalesOrderVariables.AVG_ORDER_VALUE, col(SalesOrderVariables.SUM_BASKET_VALUE) / col(SalesOrderVariables.COUNT_BASKET_VALUE))
      .withColumn(SalesOrderVariables.AVG_ORDER_ITEM_VALUE, col(SalesOrderVariables.SUM_BASKET_VALUE) / col(SalesOrderVariables.ORDER_ITEM_COUNT))
      .withColumn(SalesRuleSetVariables.AVG_COUPON_VALUE_USED, col(SalesRuleSetVariables.COUPON_SUM) / col(SalesRuleSetVariables.COUPON_COUNT))
      .withColumn(SalesRuleSetVariables.AVERAGE_DISCOUNT_USED, col(SalesRuleSetVariables.DISCOUNT_SUM) / col(SalesRuleSetVariables.DISCOUNT_COUNT))
      .drop(SalesOrderVariables.COUNT_BASKET_VALUE)
      .drop(SalesOrderVariables.COUNT_BASKET_VALUE)
      .drop(SalesOrderVariables.SUM_BASKET_VALUE)
      .drop(SalesOrderVariables.ORDER_ITEM_COUNT)
      .drop(SalesRuleSetVariables.COUPON_SUM)
      .drop(SalesRuleSetVariables.COUPON_COUNT)
      .drop(SalesRuleSetVariables.DISCOUNT_SUM)
      .drop(SalesRuleSetVariables.DISCOUNT_COUNT)
      .drop(SalesOrderItemVariables.SUCCESSFUL_ORDERS)
      .drop(SalesOrderItemVariables.FAV_BRAND)
      .drop(SalesOrderVariables.LAST_ORDER_DATE)
    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    DataWriter.writeCsv(custOrdersCsv, DataSets.VARIABLES, DataSets.CAT_AVG, DataSets.DAILY_MODE, incrDate, fileDate + "_CUST_ORDERS", DataSets.IGNORE_SAVEMODE, "true", ";")

  }

  def joinCustOrder(incr: DataFrame, prevFull: DataFrame): DataFrame = {
    var custOrdersFull: DataFrame = incr
    if (null != prevFull) {
      custOrdersFull = incr.join(prevFull, incr(SalesOrderVariables.FK_CUSTOMER) === prevFull(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
        .select(coalesce(incr(SalesOrderVariables.FK_CUSTOMER), prevFull(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
          when(incr(SalesOrderVariables.MAX_ORDER_BASKET_VALUE) > prevFull(SalesOrderVariables.MAX_ORDER_BASKET_VALUE), incr(SalesOrderVariables.MAX_ORDER_BASKET_VALUE)).otherwise(prevFull(SalesOrderVariables.MAX_ORDER_BASKET_VALUE)) as SalesOrderVariables.MAX_ORDER_BASKET_VALUE,
          when(incr(SalesOrderVariables.MAX_ORDER_ITEM_VALUE) > prevFull(SalesOrderVariables.MAX_ORDER_ITEM_VALUE), incr(SalesOrderVariables.MAX_ORDER_ITEM_VALUE)).otherwise(prevFull(SalesOrderVariables.MAX_ORDER_ITEM_VALUE)) as SalesOrderVariables.MAX_ORDER_ITEM_VALUE,
          incr(SalesOrderVariables.SUM_BASKET_VALUE) + prevFull(SalesOrderVariables.SUM_BASKET_VALUE) as SalesOrderVariables.SUM_BASKET_VALUE,
          incr(SalesOrderVariables.COUNT_BASKET_VALUE) + prevFull(SalesOrderVariables.COUNT_BASKET_VALUE) as SalesOrderVariables.COUNT_BASKET_VALUE,
          incr(SalesOrderVariables.ORDER_ITEM_COUNT) + prevFull(SalesOrderVariables.ORDER_ITEM_COUNT) as SalesOrderVariables.ORDER_ITEM_COUNT,
          coalesce(incr(SalesOrderVariables.LAST_ORDER_DATE), prevFull(SalesOrderVariables.LAST_ORDER_DATE)) as SalesOrderVariables.LAST_ORDER_DATE,
          coalesce(incr(SalesAddressVariables.LAST_SHIPPING_CITY), prevFull(SalesAddressVariables.LAST_SHIPPING_CITY)) as SalesAddressVariables.LAST_SHIPPING_CITY,
          coalesce(incr(SalesAddressVariables.LAST_SHIPPING_CITY_TIER), prevFull(SalesAddressVariables.LAST_SHIPPING_CITY_TIER)) as SalesAddressVariables.LAST_SHIPPING_CITY_TIER,
          coalesce(prevFull(SalesAddressVariables.FIRST_SHIPPING_CITY), incr(SalesAddressVariables.FIRST_SHIPPING_CITY)) as SalesAddressVariables.FIRST_SHIPPING_CITY,
          coalesce(prevFull(SalesAddressVariables.FIRST_SHIPPING_CITY_TIER), incr(SalesAddressVariables.FIRST_SHIPPING_CITY_TIER)) as SalesAddressVariables.FIRST_SHIPPING_CITY_TIER,
          prevFull(SalesOrderItemVariables.COUNT_OF_INVLD_ORDERS) + incr(SalesOrderItemVariables.COUNT_OF_INVLD_ORDERS) as SalesOrderItemVariables.COUNT_OF_INVLD_ORDERS,
          prevFull(SalesOrderItemVariables.COUNT_OF_CNCLD_ORDERS) + incr(SalesOrderItemVariables.COUNT_OF_CNCLD_ORDERS) as SalesOrderItemVariables.COUNT_OF_CNCLD_ORDERS,
          prevFull(SalesOrderItemVariables.COUNT_OF_RET_ORDERS) + incr(SalesOrderItemVariables.COUNT_OF_RET_ORDERS) as SalesOrderItemVariables.COUNT_OF_RET_ORDERS,
          prevFull(SalesOrderItemVariables.SUCCESSFUL_ORDERS) + incr(SalesOrderItemVariables.SUCCESSFUL_ORDERS) as SalesOrderItemVariables.SUCCESSFUL_ORDERS,
          when(incr(SalesRuleSetVariables.MIN_COUPON_VALUE_USED) < prevFull(SalesRuleSetVariables.MIN_COUPON_VALUE_USED), incr(SalesRuleSetVariables.MIN_COUPON_VALUE_USED)).otherwise(prevFull(SalesRuleSetVariables.MIN_COUPON_VALUE_USED)) as SalesRuleSetVariables.MIN_COUPON_VALUE_USED,
          when(incr(SalesRuleSetVariables.MAX_COUPON_VALUE_USED) > prevFull(SalesRuleSetVariables.MAX_COUPON_VALUE_USED), incr(SalesRuleSetVariables.MAX_COUPON_VALUE_USED)).otherwise(prevFull(SalesRuleSetVariables.MAX_COUPON_VALUE_USED)) as SalesRuleSetVariables.MIN_COUPON_VALUE_USED,
          incr(SalesRuleSetVariables.COUPON_SUM) + prevFull(SalesRuleSetVariables.COUPON_SUM) as SalesRuleSetVariables.COUPON_SUM,
          incr(SalesRuleSetVariables.COUPON_COUNT) + prevFull(SalesRuleSetVariables.COUPON_COUNT) as SalesRuleSetVariables.COUPON_COUNT,
          when(incr(SalesRuleSetVariables.MIN_DISCOUNT_USED) < prevFull(SalesRuleSetVariables.MIN_DISCOUNT_USED), incr(SalesRuleSetVariables.MIN_DISCOUNT_USED)).otherwise(prevFull(SalesRuleSetVariables.MIN_DISCOUNT_USED)) as SalesRuleSetVariables.MIN_DISCOUNT_USED,
          when(incr(SalesRuleSetVariables.MAX_DISCOUNT_USED) < prevFull(SalesRuleSetVariables.MAX_DISCOUNT_USED), incr(SalesRuleSetVariables.MAX_DISCOUNT_USED)).otherwise(prevFull(SalesRuleSetVariables.MAX_DISCOUNT_USED)) as SalesRuleSetVariables.MAX_DISCOUNT_USED,
          incr(SalesRuleSetVariables.DISCOUNT_SUM) + prevFull(SalesRuleSetVariables.DISCOUNT_SUM) as SalesRuleSetVariables.DISCOUNT_SUM,
          incr(SalesRuleSetVariables.DISCOUNT_COUNT) + prevFull(SalesRuleSetVariables.DISCOUNT_COUNT) as SalesRuleSetVariables.DISCOUNT_COUNT,
          coalesce(incr(SalesOrderItemVariables.REVENUE_7), prevFull(SalesOrderItemVariables.REVENUE_7)) as SalesOrderItemVariables.REVENUE_7,
          coalesce(incr(SalesOrderItemVariables.REVENUE_30), prevFull(SalesOrderItemVariables.REVENUE_30)) as SalesOrderItemVariables.REVENUE_30,
          coalesce(incr(SalesOrderItemVariables.REVENUE_LIFE), prevFull(SalesOrderItemVariables.REVENUE_LIFE)) as SalesOrderItemVariables.REVENUE_LIFE,
          coalesce(incr(SalesOrderItemVariables.ORDERS_COUNT_LIFE), prevFull(SalesOrderItemVariables.ORDERS_COUNT_LIFE)) as SalesOrderItemVariables.ORDERS_COUNT_LIFE,
          coalesce(incr(SalesOrderVariables.CATEGORY_PENETRATION), prevFull(SalesOrderVariables.CATEGORY_PENETRATION)) as SalesOrderVariables.CATEGORY_PENETRATION,
          coalesce(incr(SalesOrderVariables.BRICK_PENETRATION), prevFull(SalesOrderVariables.BRICK_PENETRATION)) as SalesOrderVariables.BRICK_PENETRATION,
          coalesce(incr(SalesOrderItemVariables.FAV_BRAND), prevFull(SalesOrderItemVariables.FAV_BRAND)) as SalesOrderItemVariables.FAV_BRAND
        )
    }
    custOrdersFull
  }

  def merger(salesRevenueVariables: DataFrame, salesDiscount: DataFrame, salesInvalid: DataFrame, salesCatBrick: DataFrame, salesOrderValue: DataFrame, salesAddressFirst: DataFrame): DataFrame = {

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
        salesInvalid(SalesOrderItemVariables.SUCCESSFUL_ORDERS)
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
        catBrickJoined(SalesOrderVariables.CATEGORY_PENETRATION),
        catBrickJoined(SalesOrderVariables.BRICK_PENETRATION),
        catBrickJoined(SalesOrderItemVariables.FAV_BRAND),
        salesOrderValue(SalesOrderVariables.MAX_ORDER_BASKET_VALUE),
        salesOrderValue(SalesOrderVariables.MAX_ORDER_ITEM_VALUE),
        salesOrderValue(SalesOrderVariables.SUM_BASKET_VALUE),
        salesOrderValue(SalesOrderVariables.COUNT_BASKET_VALUE),
        salesOrderValue(SalesOrderVariables.ORDER_ITEM_COUNT)
      )

    val res = salesValueJoined.join(salesAddressFirst, salesValueJoined(SalesOrderVariables.FK_CUSTOMER) === salesAddressFirst(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
      .select(coalesce(salesAddressFirst(SalesOrderVariables.FK_CUSTOMER), salesAddressFirst(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
        salesValueJoined(SalesOrderItemVariables.REVENUE_7),
        salesValueJoined(SalesOrderItemVariables.REVENUE_30),
        salesValueJoined(SalesOrderItemVariables.REVENUE_LIFE),
        salesValueJoined(SalesOrderItemVariables.ORDERS_COUNT_LIFE),
        salesValueJoined(SalesOrderVariables.LAST_ORDER_DATE),
        salesValueJoined(SalesRuleSetVariables.MIN_COUPON_VALUE_USED),
        salesValueJoined(SalesRuleSetVariables.MAX_COUPON_VALUE_USED),
        salesValueJoined(SalesRuleSetVariables.AVG_COUPON_VALUE_USED),
        salesValueJoined(SalesRuleSetVariables.MIN_DISCOUNT_USED),
        salesValueJoined(SalesRuleSetVariables.MAX_DISCOUNT_USED),
        salesValueJoined(SalesRuleSetVariables.AVERAGE_DISCOUNT_USED),
        salesValueJoined(SalesOrderItemVariables.COUNT_OF_INVLD_ORDERS),
        salesValueJoined(SalesOrderItemVariables.COUNT_OF_CNCLD_ORDERS),
        salesValueJoined(SalesOrderItemVariables.COUNT_OF_RET_ORDERS),
        salesValueJoined(SalesOrderItemVariables.SUCCESSFUL_ORDERS),
        salesValueJoined(SalesOrderVariables.CATEGORY_PENETRATION),
        salesValueJoined(SalesOrderVariables.BRICK_PENETRATION),
        salesValueJoined(SalesOrderItemVariables.FAV_BRAND),
        salesValueJoined(SalesOrderVariables.MAX_ORDER_BASKET_VALUE),
        salesValueJoined(SalesOrderVariables.MAX_ORDER_ITEM_VALUE),
        salesValueJoined(SalesOrderVariables.SUM_BASKET_VALUE),
        salesValueJoined(SalesOrderVariables.COUNT_BASKET_VALUE),
        salesValueJoined(SalesOrderVariables.ORDER_ITEM_COUNT),
        salesAddressFirst(SalesAddressVariables.FIRST_SHIPPING_CITY),
        salesAddressFirst(SalesAddressVariables.FIRST_SHIPPING_CITY_TIER),
        salesAddressFirst(SalesAddressVariables.LAST_SHIPPING_CITY),
        salesAddressFirst(SalesAddressVariables.LAST_SHIPPING_CITY_TIER)
      )
    res
  }

  def readDF(incrDate: String, prevDate: String, paths: String): HashMap[String, DataFrame] = {

    var dfMap = new HashMap[String, DataFrame]()

    var mode: String = DataSets.FULL_MERGE_MODE
    if(null == paths){
      mode = DataSets.DAILY_MODE
    }
    val custOrdersPrevFull = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_ORDERS, DataSets.FULL_MERGE_MODE, prevDate)
    dfMap.put("custOrdersPrevFull", custOrdersPrevFull)
    val orderStatusmap = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_INVALID_CANCEL, DataSets.FULL_MERGE_MODE, prevDate)
    dfMap.put("orderStatusmap", orderStatusmap)
    val salesOrderIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, mode, incrDate)
    dfMap.put("salesOrderIncr", salesOrderIncr)
    val salesOrderItemIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ITEM, mode, incrDate)
    dfMap.put("salesOrderItemIncr", salesOrderItemIncr)
    val salesOrderFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.FULL_MERGE_MODE, incrDate)
    dfMap.put("salesOrderFull", salesOrderFull)
    val salesRuleFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_RULE, DataSets.FULL_MERGE_MODE, incrDate)
    dfMap.put("salesRuleFull", salesRuleFull)
    val salesRuleSetFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_RULE_SET, DataSets.FULL_MERGE_MODE, incrDate)
    dfMap.put("salesRuleSetFull", salesRuleSetFull)
    val salesAddressFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ADDRESS, DataSets.FULL_MERGE_MODE, incrDate)
    dfMap.put("salesAddressFull", salesAddressFull)
    val custTop5Incr = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_TOP5, mode, incrDate)
    dfMap.put("custTop5Incr", custTop5Incr)
    val cityZoneFull = DataReader.getDataFrame4mCsv(ConfigConstants.ZONE_CITY_PINCODE_PATH, "true", ",")
    dfMap.put("cityZoneFull", cityZoneFull)
    val salesRevenuePrevFull = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.FULL_MERGE_MODE, prevDate)
    dfMap.put("salesRevenuePrevFull", salesRevenuePrevFull)
    val before7 = TimeUtils.getDateAfterNDays(-7, prevDate)
    val salesRevenue7 = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.DAILY_MODE, before7)
    dfMap.put("salesRevenue7", salesRevenue7)
    val before30 = TimeUtils.getDateAfterNDays(-30, prevDate)
    val salesRevenue30 = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.DAILY_MODE, before30)
    dfMap.put("salesRevenue30", salesRevenue30)
    val before90 = TimeUtils.getDateAfterNDays(-90, prevDate)
    val salesRevenue90 = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.DAILY_MODE, before90)
    dfMap.put("salesRevenue90", salesRevenue90)
    val salesOrderIncrFil = Utils.getOneDayData(salesOrderIncr, SalesOrderVariables.CREATED_AT, incrDate, TimeConstants.DATE_FORMAT_FOLDER)
    dfMap.put("salesOrderIncrFil", salesOrderIncrFil)
    val salesOrderItemIncrFil = Utils.getOneDayData(salesOrderItemIncr, SalesOrderVariables.CREATED_AT, incrDate, TimeConstants.DATE_FORMAT_FOLDER)
    dfMap.put("salesOrderItemIncrFil", salesOrderItemIncrFil)
    dfMap
  }

}
