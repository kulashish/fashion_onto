package com.jabong.dap.common.udf

import java.sql.{ Date, Timestamp }

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

import scala.collection.mutable
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
  val columnCount = udf((colList: List[String]) => UdfUtils.getCountColumn(colList: List[String]))

  /**
   * concatenate two List
   */
  val concatenateRecSkuList = udf((l1: scala.collection.mutable.ArrayBuffer[String], l2: scala.collection.mutable.ArrayBuffer[String]) => UdfUtils.concatenateRecSkuList(l1: scala.collection.mutable.ArrayBuffer[String], l2: scala.collection.mutable.ArrayBuffer[String]))

  /**
   * Removes all zeroes string and null string to emptyString.
   */
  val removeAllZero = udf((str: String) => UdfUtils.removeAllZero(str: String))

  val allZero2NullUdf = udf((str: String) => UdfUtils.allZero2Null(str: String))

  val timeToSlot = udf((str: String, dateFormat: String) => UdfUtils.timeToSlot(str: String, dateFormat: String))

  /**
   * For populating empty email id from dcf data as _app_deviceid
   */
  val populateEmail = udf((email: String, deviceid: String) => UdfUtils.dcfEmail(email: String, deviceid: String))

  /**
   * toLong will convert String data to Long data
   */
  val toLong = udf((str: String) => UdfUtils.getToLong(str: String))

  val bigDecimal2Double = udf((d: java.math.BigDecimal) => UdfUtils.bigDecimal2Double(d: java.math.BigDecimal))

  val udfEmailOptInStatus = udf((nls_email: String, status: String) => UdfUtils.getEmailOptInStatus(nls_email: String, status: String))

  /**
   * email will return s1 if either s is empty or null
   */
  val email = udf((s: String, s1: String) => UdfUtils.email(s: String, s1: String))

  val device = udf((s: String, s1: String, s2: String) => UdfUtils.device(s: String, s1: String, s2: String))

  val domain = udf((s: String, s1: String) => UdfUtils.domain(s: String, s1: String))

  val successOrder = udf((i: Long) => UdfUtils.successOrder(i: Long))

  val getElementArray = udf((a: ArrayBuffer[String], i: Int) => UdfUtils.getElementArray(a: ArrayBuffer[String], i: Int))

  val getElementList = udf((a: mutable.MutableList[String], i: Int) => UdfUtils.getElementList(a: mutable.MutableList[String], i: Int))

  val getElementInTupleArray = udf((a: ArrayBuffer[(Row)], i: Int, value: Int) => UdfUtils.getElementInTupleArray(a: ArrayBuffer[(Row)], i: Int, value: Int))

  val getElementInTupleList = udf((a: mutable.MutableList[(Row)], i: Int, value: Int) => UdfUtils.getElementInTupleList(a: mutable.MutableList[(Row)], i: Int, value: Int))

  val toLowercase = udf((s: String) => UdfUtils.toLower(s: String))

  val addString = udf((s: String, constant: String) => UdfUtils.addString(s: String, constant: String))

  val addInt = udf((i1: Int, i2: Int) => UdfUtils.addInt(i1: Int, i2: Int))

  val dateCsvFormat = udf((s: Timestamp) => UdfUtils.csvDateFormat(s: Timestamp))

  val isEquals = udf((d1: Any, d2: Any) => UdfUtils.isEquals(d1: Any, d2: Any))

  val dnd = udf((s: String) => UdfUtils.markDnd(s: String))

  val mps = udf((s: String) => UdfUtils.markMps(s: String))

  val followUpCampaignMailType = udf((mailType: Int) => UdfUtils.followUpCampaignMailTypes(mailType: Int))

  val BigDecimalToDouble = udf((value: java.math.BigDecimal) => UdfUtils.BigDecimalToDouble(value: java.math.BigDecimal))

  val platinumStatus = udf((s: String) => UdfUtils.platinumStatus(s: String))

  val nextPriceBand = udf((priceBand: String) => UdfUtils.nextPriceBand(priceBand: String))

  val getLatestEmailOpenDate = udf((s: String, s1: String, s2: String, s3: String) => UdfUtils.latestEmailOpenDate(s: String, s1: String, s2: String, s3: String))

  val columnAsArraySize = udf((colList: mutable.MutableList[String]) => UdfUtils.size(colList: scala.collection.mutable.MutableList[String]))

  val nonBeauty = udf((category: String, created_at: Timestamp) => UdfUtils.nonBeauty(category: String, created_at: Timestamp))

  val beauty = udf((category: String, created_at: Timestamp) => UdfUtils.beauty(category: String, created_at: Timestamp))

  val lengthString = udf((string: String) => UdfUtils.lengthString(string: String))

  val getAcartNumberOfSkus = udf((string: String) => UdfUtils.acartNumberOfSkus(string: String))

  val maskForDecrypt = udf((col: String, mask: String) => UdfUtils.addMaskString(col: String, mask: String))

  val dateModeFilter = udf((created_at: Timestamp, incrDate: String, n: Int, modBase: Int) => UdfUtils.modeFilter(created_at: Timestamp, incrDate: String, n: Int, modBase: Int))
  // val mergeMap = udf((prevMap:  scala.collection.immutable.Map[String, Row], newMap: scala.collection.immutable.Map[String, Row]) => UdfUtils.mergeMaps(prevMap, newMap))
}
