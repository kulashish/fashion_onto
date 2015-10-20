package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{SalesOrderVariables, NewsletterPreferencesVariables, ContactListMobileVars, NewsletterVariables}
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.order.variables.SalesOrderItem

import org.apache.spark.sql.DataFrame

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
object CustomerOrders {

  def start(vars: ParamInfo) = {
    val saveMode = vars.saveMode
    val fullPath = OptionUtils.getOptValue(vars.path)
    val incrDate = OptionUtils.getOptValue(vars.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val prevDate = OptionUtils.getOptValue(vars.fullDate, TimeUtils.getDateAfterNDays(-2, TimeConstants.DATE_FORMAT_FOLDER))

    val (salesOrder, salesOrderItem, salesRule, salesRuleSet, salesAddress, itr, cityZone, salesRevenuePrevFull, salesRevenue7, salesRevenue30, salesRevenue90, salesRuleCalc, salesItemInvalidCalc, salesCatBrickCalc, salesOrderValueCalc, custOrdersPrevFull) = readDf(incrDate, prevDate)

    val salesOrderNew = salesOrder.na.fill(Map(
      SalesOrderVariables.GW_AMOUNT -> 0.0
    ))
    val saleOrderJoined = salesOrderNew.join(salesOrderItem, salesOrderNew(SalesOrderVariables.ID_SALES_ORDER) === salesOrderItem(SalesOrderVariables.FK_SALES_ORDER))

    val (joinedData, salesRevenueVariables) = SalesOrderItem.getRevenueOrdersCount(saleOrderJoined, salesRevenuePrevFull, salesRevenue7, salesRevenue30, salesRevenue90)
    var savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(salesRevenueVariables, savePath, DataSets.DAILY_MODE)


    val salesDiscount = SalesOrderItem.getCouponDisc(salesOrder, salesRule, salesRuleSet, salesRuleCalc)
    savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_COUPON_DISC, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(salesDiscount, savePath, DataSets.DAILY_MODE)

    val salesInvalid = SalesOrderItem.getInvalidCancelOrders(saleOrderJoined, salesItemInvalidCalc)

    savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_INVALID_CANCEL, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(salesInvalid, savePath, DataSets.DAILY_MODE)

    val (salesCatBrick, joined) = SalesOrderItem.getCatBrickPen(saleOrderJoined, itr, salesCatBrickCalc)
    savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_CAT_BRICK_PEN, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(joined, savePath, DataSets.DAILY_MODE)

    val salesOrderValue = SalesOrderItem.getOrderValue(saleOrderJoined, salesOrderValueCalc)

    savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_ORDERS_VALUE, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(salesOrderValue, savePath, DataSets.DAILY_MODE)


    val mergedVariables = merger(salesRevenueVariables, salesDiscount, salesInvalid, salesCatBrick, salesOrderValue, custOrdersPrevFull)


  }


  def merger(salesRevenueVariables: DataFrame, salesDiscount: DataFrame, salesInvalid: DataFrame, salesCatBrick: DataFrame, salesOrderValue: DataFrame, prevFUll: DataFrame):DataFrame={

    null
  }

  def readDf(incrDate: String, prevDate: String): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {

    val salesOrder = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.FULL_MERGE_MODE, incrDate)
    val salesOrderItem = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_PREFERENCE, DataSets.FULL_MERGE_MODE, prevDate)
    val salesRule = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_RULE, DataSets.FULL_MERGE_MODE, prevDate)
    val salesRuleSet = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_RULE, DataSets.FULL_MERGE_MODE, prevDate)
    val salesAddress = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ORDER_ADDRESS, DataSets.FULL_MERGE_MODE, prevDate)
    val itr = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.BASIC_ITR, DataSets.FULL_MERGE_MODE, prevDate)
    val cityZone = DataReader.getDataFrame4mCsv(ConfigConstants.ZONE_CITY_PINCODE_PATH, "true", ",")

    val salesRevenuePrevFull = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.FULL_MERGE_MODE, prevDate)

    val before7 = TimeUtils.getDateAfterNDays(7, prevDate)
    val salesRevenue7 = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.FULL_MERGE_MODE, before7)

    val before30 = TimeUtils.getDateAfterNDays(30, prevDate)
    val salesRevenue30 = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.FULL_MERGE_MODE, before30)

    val before90 = TimeUtils.getDateAfterNDays(90, prevDate)
    val salesRevenue90 = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.FULL_MERGE_MODE, before90)

    val salesRuleCalc = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_COUPON_DISC, DataSets.FULL_MERGE_MODE, prevDate)

    val salesItemInvalidCalc = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_INVALID_CANCEL, DataSets.FULL_MERGE_MODE, prevDate)

    val salesCatBrickCalc = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_CAT_BRICK_PEN, DataSets.FULL_MERGE_MODE, prevDate)

    val salesOrderValueCalc = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_ORDERS_VALUE, DataSets.FULL_MERGE_MODE, prevDate)

    val custOrdersPrevFull = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_ORDERS, DataSets.FULL_MERGE_MODE, prevDate)

    (salesOrder, salesOrderItem, salesRule, salesRuleSet, salesAddress, itr, cityZone, salesRevenuePrevFull, salesRevenue7, salesRevenue30, salesRevenue90, salesRuleCalc, salesItemInvalidCalc, salesCatBrickCalc, salesOrderValueCalc, custOrdersPrevFull)
  }

}
