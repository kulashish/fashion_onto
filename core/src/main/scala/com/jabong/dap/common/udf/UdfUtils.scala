package com.jabong.dap.common.udf

import java.sql.Timestamp
import java.util.Date

import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.{ ArrayUtils, StringUtils }
import com.jabong.dap.data.storage.DataSets
import net.liftweb.json.JsonParser.ParseException
import net.liftweb.json._

import scala.collection.mutable
import scala.collection.mutable.{ WrappedArray, ListBuffer }

/**
 * Created by raghu on 3/7/15.
 */
object UdfUtils {

  /**
   * min of Timestamp t1 or t2
   * @param t1
   * @param t2
   * @return Timestamp
   */
  def getMin(t1: Timestamp, t2: Timestamp): Timestamp = {

    if (t1 == null) {
      return t2
    }

    if (t2 == null) {
      return t1
    }

    if (t1.compareTo(t2) >= 0)
      t1
    else
      t2

  }

  def toLower(s: String): String = {
    return s.toLowerCase()
  }

  /**
   * max of Timestamp t1 or t2
   * @param t1
   * @param t2
   * @return Timestamp
   */
  def getMax(t1: Timestamp, t2: Timestamp): Timestamp = {

    if (t1 == null) {
      return t2
    }

    if (t2 == null) {
      return t1
    }

    if (t1.compareTo(t2) < 0)
      t2
    else
      t1

  }

  /**
   * It will return latest value
   * @param a1
   * @param a2
   * @tparam T
   * @return T
   */
  def getLatest[T](a1: T, a2: T): T = {

    if (a2 == null) a1 else a2

  }

  /**
   * This will merge two slots data
   * @param oldSlot
   * @param newSlot
   * @return String
   */
  def getMergeSlots(oldSlot: Any, newSlot: Any): (String) = {

    if (oldSlot == null && newSlot == null) {

      return null
    }
    if (oldSlot == null) {

      return newSlot.toString
    }
    if (newSlot == null) {

      return oldSlot.toString
    }

    val oldSlotArray = oldSlot.toString.split("!")

    val newSlotArray = newSlot.toString.split("!")

    var finalSlotArray = new Array[Int](oldSlotArray.length)

    for (i <- 0 to oldSlotArray.length - 1) {

      finalSlotArray(i) = oldSlotArray(i).toInt + newSlotArray(i).toInt
    }

    return (ArrayUtils.arrayToString(finalSlotArray, 0))
  }

  /**
   * This method will return max value from slot data
   * @param oldSlot
   * @param newSlot
   * @param oldPreferredSlot
   * @return Int
   */
  def getMaxSlot(oldSlot: Any, newSlot: Any, oldPreferredSlot: Int): Int = {

    if (oldSlot == null && newSlot == null) {

      return 0
    }
    if (oldSlot == null) {

      return getMaxSlot(newSlot)
    }
    if (newSlot == null) {

      return oldPreferredSlot
    }

    var maxSlot = 0

    var maxOld = 0

    var maxNew = 0

    val oldSlotArray = oldSlot.toString.split("!")

    val newSlotArray = newSlot.toString.split("!")

    for (i <- 0 to oldSlotArray.length - 1) {

      maxNew = oldSlotArray(i).toInt + newSlotArray(i).toInt

      if (maxNew > maxOld) {

        maxOld = maxNew

        maxSlot = i + 1
      }
    }

    return maxSlot
  }

  /**
   * this method will return max value from slots
   * @param slots
   * @return Int
   */
  def getMaxSlot(slots: Any): Int = {

    if (slots == null) {
      return 0
    }

    var maxSlot = 0

    var maxOld = 0

    val slotArray = slots.toString.split("!")

    for (i <- 0 to slotArray.length - 1) {

      if (slotArray(i).toInt > maxOld) {

        maxOld = slotArray(i).toInt

        maxSlot = i + 1
      }
    }

    return maxSlot
  }

  /**
   * this method will create a slot data
   * @param iterable
   * @return Tuple2[String, Int]
   */
  def getCompleteSlotData(iterable: Iterable[(Int, Int)]): Tuple2[String, Int] = {

    var timeSlotArray = new Array[Int](12)

    var maxSlot: Int = -1

    var max: Int = -1

    iterable.foreach {
      case (slot, value) =>
        if (value > max) { maxSlot = slot; max = value }
        timeSlotArray(slot - 1) = value
    }
    new Tuple2(ArrayUtils.arrayToString(timeSlotArray, 0), maxSlot)
  }

  /**
   * This will return age of person
   * @param birthday
   * @return
   */
  def getAge(birthday: Date): Int = {

    if (birthday == null) {
      return 0
    }

    return TimeUtils.getYearFromToday(birthday)

  }

  /**
   * getAppUserId  creates new userid for users not having any userid
   * by using the browserid prepended with a constant
   *
   * @param userid
   * @param domain
   * @param browserid
   * @return either transformed userid
   */
  def getAppUserId(userid: String, domain: String, browserid: String): String = {
    var app_user_id = userid
    if (app_user_id == null && (domain == DataSets.IOS || domain == DataSets.ANDROID || domain == DataSets.WINDOWS)) {
      app_user_id = "_app_" + browserid
    }
    return app_user_id
  }
  /**
   * This will return Timestamp into YYYYMMDD format
   * @param t1
   * @return
   */
  def getYYYYmmDD(t1: Timestamp): Timestamp = {

    if (t1 == null) {
      return null
    }

    val time = t1.toString()

    return Timestamp.valueOf(time.substring(0, time.indexOf(" ") + 1) + TimeConstants.START_TIME_MS)
  }

  /**
   * This will return Timestamp into YYYYMMDD format
   * @param t1
   * @return
   */
  def getYYYYmmDD(t1: String): Timestamp = {

    if (t1 == null) {
      return null
    }

    if (t1.contains(" ")) {
      return Timestamp.valueOf(t1.substring(0, t1.indexOf(" ") + 1) + TimeConstants.START_TIME_MS)
    } else {
      return Timestamp.valueOf(t1 + " " + TimeConstants.START_TIME_MS)
    }
  }

  /**
   *  getSimpleSkuFromExtraData will extract data from extraData
   * @param extraData
   * @return
   */
  def getSimpleSkuFromExtraData(extraData: String): String = {

    //    var extraData = "{\"simple_sku\":\"LA625BG58FVTINDFAS-3949337\",\"price\":1599,\"all_colors\":\"LA625BG58FVTINDFAS\",\"sel_size_qty\":\"1\",\"id_catalog_config\":\"1251841\",\"all_simples\":{\"LA625BG58FVTINDFAS-3949337\":\"1\"}}"

    if (extraData == null)
      return null

    if (extraData.length() < 10)
      return null

    var simple_sku: String = null

    try {
      val jsonExtraData = parse(extraData)

      simple_sku = compact(render(jsonExtraData \ "simple_sku")).replaceAll("^\"|\"$", "")
    } catch {
      case ex: ParseException => {
        ex.printStackTrace()
        return null
      }
    }
    if (simple_sku == null || simple_sku.length() < 10)
      return null

    return simple_sku
  }

  /**
   * getPriceFromExtraData will extract data from extraData
   * @param extraData
   * @return
   */
  def getPriceFromExtraData(extraData: String): BigDecimal = {

    if (extraData == null)
      return 0

    if (extraData.length() < 10)
      return 0

    var priceString: String = null
    try {
      val jsonExtraData = parse(extraData)

      priceString = compact(render(jsonExtraData \ "price")).replaceAll("^\"|\"$", "")
    } catch {
      case ex: ParseException => {
        ex.printStackTrace()
        return 0
      }
    }
    if (priceString == null || priceString.length() < 1)
      return 0

    return priceString.toDouble

  }

  /**
   * getskuFromSimpleSku will convert simple_sku to sku
   * @param simpleSku
   * @return
   */
  def getskuFromSimpleSku(simpleSku: String): String = {

    if (simpleSku == null) {
      return null
    }

    if (!(simpleSku.contains('-'))) {
      return simpleSku
    }

    return simpleSku.substring(0, simpleSku.lastIndexOf('-'))
  }

  /**
   *
   * @param array
   * @tparam T
   * @return
   */
  def getDistinctList[T](array: WrappedArray[T]): List[T] = {

    if (array == null || array.isEmpty) {
      return null
    }

    val list = array.toList.distinct

    return list

  }

  /**
   *
   * @param skuArray
   * @tparam T
   * @return
   */
  def getRepeatedSku[T](skuArray: WrappedArray[T]): List[T] = {

    if (skuArray == null || skuArray.isEmpty) {
      return null
    }

    val setSkus = new mutable.HashSet[T]

    val skuList = new ListBuffer[T]()

    for (sku <- skuArray.toList) {

      if (!setSkus.contains(sku)) {
        setSkus.add(sku)
      } else {
        skuList += sku
      }
    }

    if (skuList.toList.isEmpty) {
      return null
    }

    return skuList.toList.distinct
  }

  /**
   *
   * @param skuList
   * @tparam T
   * @return
   */
  def getCountSku[T](skuList: List[T]): Int = {

    if (skuList == null || skuList.isEmpty) {
      return 0
    }

    return skuList.length
  }

  /**
   * returns dayName with max click given counts for 7 days
   * @param count1
   * @param count2
   * @param count3
   * @param count4
   * @param count5
   * @param count6
   * @param count7
   * @return
   */
  def getMaxClickDayName(count1: Int, count2: Int, count3: Int, count4: Int, count5: Int, count6: Int, count7: Int): String = {
    var max = count1
    var index = 0

    if (max < count2) {
      max = count2
      index = 1
    }
    if (max < count3) {
      max = count3
      index = 2
    }
    if (max < count4) {
      max = count4
      index = 3
    }
    if (max < count5) {
      max = count5
      index = 4
    }
    if (max < count6) {
      max = count6
      index = 5
    }
    if (max < count7) {
      max = count7
      index = 6
    }
    return TimeUtils.nextNDay("Monday", index)
  }

  /**
   * Returns empty string if the string contains all zeros or null.
   * @param str
   * @return String
   */
  def removeAllZero(str: String): String = {
    if (null == str || StringUtils.isAllZero(str)) {
      return ""
    }
    return str
  }

  /**
   * convert string to long
   * @param str
   * @return
   */
  def getToLong(str: String): Long = {

    if (str == null) {
      return 0
    }
    try {
      return str.toLong
    } catch {
      case ex: NumberFormatException => {
        ex.printStackTrace()
        return 0
      }
    }
  }

  /**
   * For populating empty email id from dcf as _app_deviceid
   * @param email
   * @param deviceid
   * @return
   */
  def dcfEmail(email: String, deviceid: String): String = {
    if (StringUtils.isEmpty(email))
      return "_app_" + deviceid
    return email
  }

  def email(s: String, s1: String): String = {
    if (null == s || s.equals(""))
      s1
    else
      s
  }

  def device(s: String, s1: String, s2: String): String = {
    if (null != s && (s.contains(DataSets.WINDOWS) || s.contains(DataSets.ANDROID) | s.contains(DataSets.IOS))) s1 else s2
  }

  def domain(s: String, s1: String): String = {
    if (null != s && (s.contains(DataSets.WINDOWS) || s.contains(DataSets.ANDROID) | s.contains(DataSets.IOS))) s else s1
  }

  def successOrder(i: Int): Int = {
    val successCodes = Array(3, 4, 5, 6, 7, 11, 17, 24, 33, 34)
    if (successCodes.contains(i)) {
      return 1
    } else {
      return 0
    }
  }

  def allZero2Null(str: String): String = {
    val nullStr: String = null
    if (null != str) {
      var str1 = str.trim()
      if (str1.length <= 0 || str1.matches("^[0]*")) {
        return nullStr
      } else {
        return str1
      }
    }
    str
  }

}
