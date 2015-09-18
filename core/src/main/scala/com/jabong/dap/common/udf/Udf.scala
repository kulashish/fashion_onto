package com.jabong.dap.common.udf

import java.sql.{ Date, Timestamp }

import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
 * Created by raghu on 3/7/15.
 */
object Udf {

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
   * appUsers will convert null userid  to  _app_+browserid
   */
  val appUserId = udf((userid: String, domain: String, browserid: String) => UdfUtils.getAppUserId(userid: String, domain: String, browserid: String))

  val maxClickDayName = udf((count1: Int, count2: Int, count3: Int, count4: Int, count5: Int, count6: Int, count7: Int) => UdfUtils.getMaxClickDayName(count1: Int, count2: Int, count3: Int, count4: Int, count5: Int, count6: Int, count7: Int))

  /**
   * yyyymmdd will convert yyyymmdd formate
   */
  val yyyymmdd = udf((t1: Timestamp) => UdfUtils.getYYYYmmDD(t1: Timestamp))

  /**
   * yyyymmdd will convert yyyymmdd formate
   */
  val yyyymmddString = udf((t1: String) => UdfUtils.getYYYYmmDD(t1: String))

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

  /**
   * distinctSku will return list of distinct Sku
   */
  val distinctList = udf((skuArrayBuffer: ArrayBuffer[String]) => UdfUtils.getDistinctList(skuArrayBuffer: ArrayBuffer[String]))

  /**
   * repeatedSku will return list of repeated Sku
   */
  val repeatedSku = udf((skuArrayBuffer: ArrayBuffer[String]) => UdfUtils.getRepeatedSku(skuArrayBuffer: ArrayBuffer[String]))

  /**
   * countSku will return total no of sku
   */
  val countSku = udf((skuArrayBuffer: List[String]) => UdfUtils.getCountSku(skuArrayBuffer: List[String]))

  /**
   * Removes all zeroes string and null string to emptyString.
   */
  val removeAllZero = udf((str: String) => UdfUtils.removeAllZero(str: String))

  /**
   * For populating empty email id from dcf data as _app_deviceid
   */
  val populateEmail = udf((email: String, deviceid: String) => UdfUtils.dcfEmail(email: String, deviceid: String))

  /**
   * toLong will convert String data to Long data
   */
  val toLong = udf((str: String) => UdfUtils.getToLong(str: String))

  /**
   * email will return s1 if either s is empty or null
   */
  val email = udf((s: String, s1: String) => UdfUtils.email(s: String, s1: String))

  val device = udf((s: String, s1: String, s2: String) => UdfUtils.device(s: String, s1: String, s2: String))

  val domain = udf((s: String, s1: String) => UdfUtils.domain(s: String, s1: String))

  val successOrder = udf((i: Int) => UdfUtils.successOrder(i: Int))

  val getElementArray = udf((a: Array[String], i: Int) => UdfUtils.getElementArray(a: Array[String], i: Int))
}
