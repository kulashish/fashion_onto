package com.jabong.dap.common.udf

import java.sql.{ Date, Timestamp }
import com.jabong.dap.common.constants.variables.CustomerProductShortlistVariables
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 3/7/15.
 */
object Udf {

  //  val hiveContext = Spark.getHiveContext()
  //  import hiveContext.implicits._
  // Define User Defined Functions
  //  val sqlContext = Spark.getSqlContext()

  /**
   * minTimestamp will return min of Timestamp t1 or t2
   */
  val minTimestamp = udf((t1: Timestamp, t2: Timestamp) => UdfUtils.getMin(t1: Timestamp, t2: Timestamp))

  /**
   * maxTimestamp will return max of Timestamp t1 or t2
   */
  val maxTimestamp = udf((t1: Timestamp, t2: Timestamp) => UdfUtils.getMax(t1: Timestamp, t2: Timestamp))

  /**
   * latestTimestamp will return latest Timestamp value
   */
  val latestTimestamp = udf((a1: Timestamp, a2: Timestamp) => UdfUtils.getLatest(a1: Timestamp, a2: Timestamp))

  /**
   * latestInt will return latest Integer value
   */
  val latestInt = udf((a1: Integer, a2: Integer) => UdfUtils.getLatest(a1: Integer, a2: Integer))

  /**
   * latestBool will return latest Boolean value
   */
  val latestBool = udf((a1: Boolean, a2: Boolean) => UdfUtils.getLatest(a1: Boolean, a2: Boolean))

  /**
   * latestDecimal will return latest Decimal value
   */
  val latestDecimal = udf((a1: java.math.BigDecimal, a2: java.math.BigDecimal) => UdfUtils.getLatest(a1: java.math.BigDecimal, a2: java.math.BigDecimal))

  /**
   * latestDate will return latest Date value
   */
  val latestDate = udf((a1: Date, a2: Date) => UdfUtils.getLatest(a1: Date, a2: Date))

  /**
   * latestString will return latest String value
   */
  val latestString = udf((a1: String, a2: String) => UdfUtils.getLatest(a1: String, a2: String))

  /**
   * mergeSlots will return merge two slots data
   */
  val mergeSlots = udf((oldSlot: Any, newSlot: Any) => UdfUtils.getMergeSlots(oldSlot: Any, newSlot: Any))

  /**
   * maxSlot will return Max Slot from two slots
   */
  val maxSlot = udf((oldSlot: Any, newSlot: Any, oldPreferredSlot: Int) => UdfUtils.getMaxSlot(oldSlot: Any, newSlot: Any, oldPreferredSlot: Int))

  /**
   * age will convert birthday to age
   */
  val age = udf((birthday: Date) => UdfUtils.getAge(birthday: Date))

  /**
   * yyyymmdd will convert yyyymmdd formate
   */
  val yyyymmdd = udf((t1: Timestamp) => UdfUtils.getYYYYmmDD(t1: Timestamp))

  /**
   * simpleSkuFromExtraData will extract data from extraData
   */
  val simpleSkuFromExtraData = udf((extraData: String) => UdfUtils.getSimpleSkuFromExtraData(extraData: String))

  /**
   * priceFromExtraData will extract data from extraData
   */
  val priceFromExtraData = udf((extraData: String) => UdfUtils.getPriceFromExtraData(extraData: String))

  /**
   * skuFromSimpleSku will convert simple_sku to sku
   */
  val skuFromSimpleSku = udf((simpleSku: String) => UdfUtils.getskuFromSimpleSku(simpleSku: String))
}
