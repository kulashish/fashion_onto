package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.{Utils, OptionUtils}
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{SalesAddressVariables, SalesOrderVariables, SalesRuleSetVariables, SalesOrderItemVariables}
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import org.apache.spark.sql.functions._

import com.jabong.dap.model.order.variables.{SalesOrderAddress, SalesOrderItem}

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

    val (salesOrderIncr, salesOrderItemIncr, salesRuleFull, salesRuleSetFull, salesAddressFUll, itr, cityZone, salesRevenuePrevFull, salesRevenue7, salesRevenue30, salesRevenue90, salesRuleCalc, salesItemInvalidCalc, salesCatBrickCalc, salesOrderValueCalc, salesAddressCalc, custOrdersPrevFull) = readDf(incrDate, prevDate)

    val salesOrderincr = Utils.getOneDayData(salesOrderIncr, SalesOrderVariables.CREATED_AT, incrDate, TimeConstants.DATE_FORMAT_FOLDER)
    val salesOrderItemincr = Utils.getOneDayData(salesOrderItemIncr, SalesOrderVariables.CREATED_AT, incrDate, TimeConstants.DATE_FORMAT_FOLDER)
    val salesOrderNew = salesOrderincr.na.fill(Map(
      SalesOrderVariables.GW_AMOUNT -> 0.0
    ))
    val saleOrderJoined = salesOrderNew.join(salesOrderItemincr, salesOrderNew(SalesOrderVariables.ID_SALES_ORDER) === salesOrderItemincr(SalesOrderVariables.FK_SALES_ORDER))

    val (joinedData, salesRevenueVariables) = SalesOrderItem.getRevenueOrdersCount(saleOrderJoined, salesRevenuePrevFull, salesRevenue7, salesRevenue30, salesRevenue90)
    var savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(salesRevenueVariables, savePath, saveMode)


    val salesDiscount = SalesOrderItem.getCouponDisc(salesOrderIncr, salesRuleFull, salesRuleSetFull, salesRuleCalc)
    savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_COUPON_DISC, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(salesDiscount, savePath, saveMode)

    val salesInvalid = SalesOrderItem.getInvalidCancelOrders(saleOrderJoined, salesItemInvalidCalc)

    savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_INVALID_CANCEL, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(salesInvalid, savePath, saveMode)

    val (salesCatBrick, joined) = SalesOrderItem.getCatBrickPen(saleOrderJoined, itr, salesCatBrickCalc)
    savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_CAT_BRICK_PEN, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(joined, savePath, saveMode)

    val salesOrderValue = SalesOrderItem.getOrderValue(saleOrderJoined, salesOrderValueCalc)

    savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_ORDERS_VALUE, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(salesOrderValue, savePath, saveMode)

    val salesAddressFirst = SalesOrderAddress.getFirstShippingCity(salesOrderIncr, salesAddressFUll, salesAddressCalc, cityZone)
    savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ADDRESS_FIRST, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(salesAddressFirst, savePath, saveMode)

    val custOrdersincr = merger(salesRevenueVariables, salesDiscount, salesInvalid, salesCatBrick, salesOrderValue, salesAddressFirst)

    val custOrderFull = joinCustOrder(custOrdersincr, custOrdersPrevFull)

    savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_ORDERS, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(custOrdersincr, savePath, saveMode)


  }

  def joinCustOrder(incr: DataFrame, prevFull: DataFrame): DataFrame={
    if(null == prevFull){
      return incr
    }
    return null
  }


  def merger(salesRevenueVariables: DataFrame, salesDiscount: DataFrame, salesInvalid: DataFrame, salesCatBrick: DataFrame, salesOrderValue: DataFrame, salesAddressFirst:DataFrame):DataFrame={

    val revJoined = salesRevenueVariables.join(salesDiscount, salesDiscount(SalesOrderVariables.FK_CUSTOMER) === salesRevenueVariables(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
                      .select(coalesce(salesDiscount(SalesOrderVariables.FK_CUSTOMER), salesRevenueVariables(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
                              salesRevenueVariables(SalesOrderItemVariables.REVENUE_7),
                              salesRevenueVariables(SalesOrderItemVariables.REVENUE_30),
                              salesRevenueVariables(SalesOrderItemVariables.REVENUE_LIFE),
                              salesRevenueVariables(SalesOrderItemVariables.ORDERS_COUNT_LIFE),
                              salesDiscount(SalesRuleSetVariables.MIN_COUPON_VALUE_USED),
                              salesDiscount(SalesRuleSetVariables.MAX_COUPON_VALUE_USED),
                              salesDiscount(SalesRuleSetVariables.COUPON_SUM)/salesDiscount(SalesRuleSetVariables.COUPON_COUNT) as SalesRuleSetVariables.AVG_COUPON_VALUE_USED,
                              salesDiscount(SalesRuleSetVariables.MIN_DISCOUNT_USED),
                              salesDiscount(SalesRuleSetVariables.MAX_DISCOUNT_USED),
                              salesDiscount(SalesRuleSetVariables.DISCOUNT_SUM)/salesDiscount(SalesRuleSetVariables.DISCOUNT_COUNT) as SalesRuleSetVariables.AVERAGE_DISCOUNT_USED)

    val invalidJoined = revJoined.join(salesInvalid, salesInvalid(SalesOrderVariables.FK_CUSTOMER) === revJoined(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
                          .select(coalesce(revJoined(SalesOrderVariables.FK_CUSTOMER), salesInvalid(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
                              revJoined(SalesOrderItemVariables.REVENUE_7),
                              revJoined(SalesOrderItemVariables.REVENUE_30),
                              revJoined(SalesOrderItemVariables.REVENUE_LIFE),
                              revJoined(SalesOrderItemVariables.ORDERS_COUNT_LIFE),
                              revJoined(SalesRuleSetVariables.MIN_COUPON_VALUE_USED),
                              revJoined(SalesRuleSetVariables.MAX_COUPON_VALUE_USED),
                              revJoined(SalesRuleSetVariables.AVG_COUPON_VALUE_USED),
                              revJoined(SalesRuleSetVariables.MIN_DISCOUNT_USED),
                              revJoined(SalesRuleSetVariables.MAX_DISCOUNT_USED),
                              revJoined(SalesRuleSetVariables.AVERAGE_DISCOUNT_USED),
                              salesInvalid(SalesOrderItemVariables.COUNT_OF_INVLD_ORDERS),
                              salesInvalid(SalesOrderItemVariables.COUNT_OF_CNCLD_ORDERS),
                              salesInvalid(SalesOrderItemVariables.COUNT_OF_RET_ORDERS)
                              )

    val catBrickJoined = invalidJoined.join(salesCatBrick, salesCatBrick(SalesOrderVariables.FK_CUSTOMER)=== invalidJoined(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
                            .select(coalesce(invalidJoined(SalesOrderVariables.FK_CUSTOMER), salesCatBrick(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
                                invalidJoined(SalesOrderItemVariables.REVENUE_7),
                                invalidJoined(SalesOrderItemVariables.REVENUE_30),
                                invalidJoined(SalesOrderItemVariables.REVENUE_LIFE),
                                invalidJoined(SalesOrderItemVariables.ORDERS_COUNT_LIFE),
                                invalidJoined(SalesRuleSetVariables.MIN_COUPON_VALUE_USED),
                                invalidJoined(SalesRuleSetVariables.MAX_COUPON_VALUE_USED),
                                invalidJoined(SalesRuleSetVariables.AVG_COUPON_VALUE_USED),
                                invalidJoined(SalesRuleSetVariables.MIN_DISCOUNT_USED),
                                invalidJoined(SalesRuleSetVariables.MAX_DISCOUNT_USED),
                                invalidJoined(SalesRuleSetVariables.AVERAGE_DISCOUNT_USED),
                                invalidJoined(SalesOrderItemVariables.COUNT_OF_INVLD_ORDERS),
                                invalidJoined(SalesOrderItemVariables.COUNT_OF_CNCLD_ORDERS),
                                invalidJoined(SalesOrderItemVariables.COUNT_OF_RET_ORDERS),
                                salesCatBrick(SalesOrderVariables.CATEGORY_PENETRATION),
                                salesCatBrick(SalesOrderVariables.BRICK_PENETRATION)
                                )
    val salesValueJoined = salesOrderValue.join(catBrickJoined, catBrickJoined(SalesOrderVariables.FK_CUSTOMER) === salesOrderValue(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
                              .select(coalesce(catBrickJoined(SalesOrderVariables.FK_CUSTOMER), salesOrderValue(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
                                    catBrickJoined(SalesOrderItemVariables.REVENUE_7),
                                    catBrickJoined(SalesOrderItemVariables.REVENUE_30),
                                    catBrickJoined(SalesOrderItemVariables.REVENUE_LIFE),
                                    catBrickJoined(SalesOrderItemVariables.ORDERS_COUNT_LIFE),
                                    catBrickJoined(SalesRuleSetVariables.MIN_COUPON_VALUE_USED),
                                    catBrickJoined(SalesRuleSetVariables.MAX_COUPON_VALUE_USED),
                                    catBrickJoined(SalesRuleSetVariables.AVG_COUPON_VALUE_USED),
                                    catBrickJoined(SalesRuleSetVariables.MIN_DISCOUNT_USED),
                                    catBrickJoined(SalesRuleSetVariables.MAX_DISCOUNT_USED),
                                    catBrickJoined(SalesRuleSetVariables.AVERAGE_DISCOUNT_USED),
                                    catBrickJoined(SalesOrderItemVariables.COUNT_OF_INVLD_ORDERS),
                                    catBrickJoined(SalesOrderItemVariables.COUNT_OF_CNCLD_ORDERS),
                                    catBrickJoined(SalesOrderItemVariables.COUNT_OF_RET_ORDERS),
                                    catBrickJoined(SalesOrderVariables.CATEGORY_PENETRATION),
                                    catBrickJoined(SalesOrderVariables.BRICK_PENETRATION),
                                    salesOrderValue(SalesOrderVariables.MAX_ORDER_BASKET_VALUE),
                                    salesOrderValue(SalesOrderVariables.MAX_ORDER_ITEM_VALUE),
                                    salesOrderValue(SalesOrderVariables.SUM_BASKET_VALUE)/salesOrderValue(SalesOrderVariables.COUNT_BASKET_VALUE) as SalesOrderVariables.AVG_ORDER_VALUE,
                                    salesOrderValue(SalesOrderVariables.SUM_BASKET_VALUE)/salesOrderValue(SalesOrderVariables.ORDER_ITEM_COUNT) as SalesOrderVariables.AVG_ORDER_ITEM_VALUE
                                    )

    val res = salesValueJoined.join(salesAddressFirst, salesValueJoined(SalesOrderVariables.FK_CUSTOMER) === salesAddressFirst(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
                                  .select(coalesce(salesAddressFirst(SalesOrderVariables.FK_CUSTOMER), salesAddressFirst(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
                                      salesValueJoined(SalesOrderItemVariables.REVENUE_7),
                                      salesValueJoined(SalesOrderItemVariables.REVENUE_30),
                                      salesValueJoined(SalesOrderItemVariables.REVENUE_LIFE),
                                      salesValueJoined(SalesOrderItemVariables.ORDERS_COUNT_LIFE),
                                      salesValueJoined(SalesRuleSetVariables.MIN_COUPON_VALUE_USED),
                                      salesValueJoined(SalesRuleSetVariables.MAX_COUPON_VALUE_USED),
                                      salesValueJoined(SalesRuleSetVariables.AVG_COUPON_VALUE_USED),
                                      salesValueJoined(SalesRuleSetVariables.MIN_DISCOUNT_USED),
                                      salesValueJoined(SalesRuleSetVariables.MAX_DISCOUNT_USED),
                                      salesValueJoined(SalesRuleSetVariables.AVERAGE_DISCOUNT_USED),
                                      salesValueJoined(SalesOrderItemVariables.COUNT_OF_INVLD_ORDERS),
                                      salesValueJoined(SalesOrderItemVariables.COUNT_OF_CNCLD_ORDERS),
                                      salesValueJoined(SalesOrderItemVariables.COUNT_OF_RET_ORDERS),
                                      salesValueJoined(SalesOrderVariables.CATEGORY_PENETRATION),
                                      salesValueJoined(SalesOrderVariables.BRICK_PENETRATION),
                                      salesValueJoined(SalesOrderVariables.MAX_ORDER_BASKET_VALUE),
                                      salesValueJoined(SalesOrderVariables.MAX_ORDER_ITEM_VALUE),
                                      salesValueJoined(SalesOrderVariables.AVG_ORDER_VALUE),
                                      salesValueJoined(SalesOrderVariables.AVG_ORDER_ITEM_VALUE),
                                      salesAddressFirst(SalesAddressVariables.FIRST_SHIPPING_CITY),
                                      salesAddressFirst(SalesAddressVariables.FIRST_SHIPPING_CITY_TIER),
                                      salesAddressFirst(SalesAddressVariables.LAST_SHIPPING_CITY),
                                      salesAddressFirst(SalesAddressVariables.LAST_SHIPPING_CITY_TIER)
                                      )

    res
  }

  def readDf(incrDate: String, prevDate: String): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {

    val custOrdersPrevFull = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_ORDERS, DataSets.DAILY_MODE, prevDate)
    var mode:String = DataSets.DAILY_MODE
    if(null == custOrdersPrevFull){
      mode = DataSets.FULL
    }
    val salesOrderIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, mode, incrDate)
    val salesOrderItemIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ITEM, mode, incrDate)
    val salesRuleFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_RULE, DataSets.FULL, incrDate)
    val salesRuleSetFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_RULE_SET, DataSets.FULL, incrDate)
    val salesAddressFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ADDRESS, DataSets.FULL, incrDate)
    val itr = CampaignInput.loadYesterdayItrSimpleData(incrDate)
    val cityZone = DataReader.getDataFrame4mCsv(ConfigConstants.ZONE_CITY_PINCODE_PATH, "true", ",")

    val salesRevenuePrevFull = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.DAILY_MODE, prevDate)

    val before7 = TimeUtils.getDateAfterNDays(-7, prevDate)
    val salesRevenue7 = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.DAILY_MODE, before7)

    val before30 = TimeUtils.getDateAfterNDays(-30, prevDate)
    val salesRevenue30 = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.DAILY_MODE, before30)

    val before90 = TimeUtils.getDateAfterNDays(-90, prevDate)
    val salesRevenue90 = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.DAILY_MODE, before90)

    val salesRuleCalc = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_COUPON_DISC, DataSets.DAILY_MODE, prevDate)

    val salesItemInvalidCalc = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_INVALID_CANCEL, DataSets.DAILY_MODE, prevDate)

    val salesCatBrickCalc = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_CAT_BRICK_PEN, DataSets.DAILY_MODE, prevDate)

    val salesOrderValueCalc = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_ORDERS_VALUE, DataSets.DAILY_MODE, prevDate)

    val salesAddressCalc = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ADDRESS_FIRST, DataSets.DAILY_MODE, prevDate)

    (salesOrderIncr, salesOrderItemIncr, salesRuleFull, salesRuleSetFull, salesAddressFull, itr, cityZone, salesRevenuePrevFull, salesRevenue7, salesRevenue30, salesRevenue90, salesRuleCalc, salesItemInvalidCalc, salesCatBrickCalc, salesOrderValueCalc, salesAddressCalc, custOrdersPrevFull)
  }

}
