package com.jabong.dap.common.udf

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{ Calendar, Date }

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.{ ArrayUtils, StringUtils }
import com.jabong.dap.data.storage.DataSets
import grizzled.slf4j.Logging
import net.liftweb.json.JsonParser.ParseException
import net.liftweb.json._
import org.apache.spark.sql.Row

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.{ ArrayBuffer, ListBuffer }

/**
 * Created by raghu on 3/7/15.
 */
object UdfUtils extends Logging {

  def csvDateFormat(s: Timestamp, fromTimeFormat: String = TimeConstants.DATE_TIME_FORMAT_MS, toTimeFormat: String = TimeConstants.DATE_TIME_FORMAT): String = {
    return TimeUtils.changeDateFormat(s, fromTimeFormat, toTimeFormat)
  }

  def addDefaultvalueForNull(in: String, value: String): String = {
    if (null == in || in.isEmpty) {
      return value
    } else {
      return in
    }
  }

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
    if (null != s)
      s.toLowerCase()
    else
      s
  }

  def markDnd(mNo: String): String = {
    var newId: String = null
    if (null == mNo) {
      "0"
    } else {
      "1"
    }
  }

  def markMps(mNo: String): String = {
    var newId: String = null
    if (null == mNo) {
      "I"
    } else {
      "O"
    }
  }

  def platinumStatus(rewardType: String): Int = {
    if (null != rewardType && "Platinum".equalsIgnoreCase(rewardType)) {
      1
    } else {
      0
    }
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
  def getDistinctList[T](array: ArrayBuffer[T]): List[T] = {

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
  def getRepeatedSku[T](skuArray: ArrayBuffer[T]): List[T] = {

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
   * @param colList
   * @tparam T
   * @return
   */
  def getCountColumn[T](colList: List[T]): Int = {

    if (colList == null || colList.isEmpty) {
      return 0
    }

    return colList.length
  }

  /**
   *
   * @param l1
   * @param l2
   * @return
   */
  def concatenateList[T](l1: scala.collection.mutable.ArrayBuffer[T], l2: scala.collection.mutable.ArrayBuffer[T]): scala.collection.mutable.ArrayBuffer[T] = {

    if (l1 == null) {
      return l2
    }
    if (l2 == null) {
      return l1
    }

    return l1 ++ l2
  }

  /**
   *
   * @param l1
   * @param l2
   * @return
   */
  def concatenateRecSkuList[T](l1: scala.collection.mutable.ArrayBuffer[T], l2: scala.collection.mutable.ArrayBuffer[T]): scala.collection.mutable.ArrayBuffer[T] = {
    if (l1 == null) {
      return l2
    }
    if (l2 == null) {
      return l1
    }
    val list1Length = l1.length
    val list2Length = l2.length
    if (list1Length < 8) {
      if (list2Length >= (16 - list1Length)) {
        val takeLength = 16 - list1Length
        return l1.take(list1Length) ++ l2.take(takeLength)
      } else {
        return l1.take(list1Length) ++ l2.take(list2Length)
      }
    } else if (list2Length < 8) {
      if (list1Length >= (16 - list2Length)) {
        val takeLength = 16 - list2Length
        return l1.take(takeLength) ++ l2.take(list2Length)
      } else {
        return l1.take(list1Length) ++ l2.take(list2Length)
      }
    }
    return l1.take(8) ++ l2.take(8)
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
   * EMAIL_SUBSCRIPTION_STATUS
   * iou - i: opt in(subscribed), o: opt out(when registering they have opted out), u: unsubscribed
   * @param nls_email
   * @param status
   * @return String
   */
  def getEmailOptInStatus(nls_email: String, status: String): String = {

    if (null == nls_email) {
      return "O"
    }

    status match {
      case "subscribed" => "I"
      case "unsubscribed" => "U"
      case _ => "U"
    }

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

  def bigDecimal2Double(bd: java.math.BigDecimal): Double = {
    if (null == bd) {
      return 0.0
    }
    bd.doubleValue()
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

  def successOrder(i: Long): Int = {
    val successCodes = Array(3, 4, 5, 6, 7, 11, 17, 24, 33, 34)
    if (successCodes.contains(i)) {
      1
    } else {
      0
    }
  }
  def getElementArray(strings: ArrayBuffer[String], i: Int): String = {
    if (i >= strings.size) "" else strings(i)
  }

  def getElementList(strings: mutable.MutableList[String], i: Int): String = {
    if (i >= strings.size) "" else strings(i)
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

  /**
   *
   * @param dateString
   * @param dateFormat
   * @return Int
   */
  def timeToSlot(dateString: String, dateFormat: String): Int = {

    logger.info("Enter in  timeToSlot:")
    val nullStr: Int = null.asInstanceOf[Int]
    var timeToSlotMap = new HashMap[Int, Int]
    timeToSlotMap += (7 -> 0, 8 -> 0, 9 -> 1, 10 -> 1, 11 -> 2, 12 -> 2, 13 -> 3, 14 -> 3, 15 -> 4, 16 -> 4, 17 -> 5, 18 -> 5, 19 -> 6, 20 -> 6, 21 -> 7, 22 -> 7, 23 -> 8, 0 -> 8, 1 -> 9, 2 -> 9, 3 -> 10, 4 -> 10, 5 -> 11, 6 -> 11)

    try {
      val formatter = new SimpleDateFormat(dateFormat)
      var date: java.util.Date = null
      date = formatter.parse(dateString)

      val calendar = Calendar.getInstance()
      calendar.setTime(date)
      val hours = calendar.get(Calendar.HOUR_OF_DAY)
      val timeSlot = timeToSlotMap.getOrElse(hours, 0)
      logger.info("Exit from  timeToSlot: ")
      return timeSlot
    } catch {
      case _: Throwable => return nullStr
    }
  }

  def getElementInTupleArray(strings: ArrayBuffer[Row], i: Int, value: Int): String = {
    if (i >= strings.size) "" else CampaignUtils.checkNullString(strings(i)(value))
  }

  def getElementInTupleList(strings: mutable.MutableList[Row], i: Int, value: Int): String = {
    if (i >= strings.size) "" else CampaignUtils.checkNullString(strings(i)(value))
  }

  def addString(value: String, constant: String): String = {
    if (null == value) return null else value + constant
  }

  def addInt(i1: Int, i2: Int): Int = {
    { if (null.asInstanceOf[Int] == i1) 0 else i1 } + { if (null.asInstanceOf[Int] == i2) 0 else i2 }
  }

  /**
   * isEquals checks: if data of d1 and d2 values are equals
   * @param d1
   * @param d2
   * @tparam T
   * @return
   */
  def isEquals[T](d1: T, d2: T): Boolean = {
    if (d1 == null || d2 == null)
      return false
    if (d1.equals(d2)) {
      return true
    }
    return false
  }

  def BigDecimalToDouble(value: java.math.BigDecimal): Double = {
    if (value == null) return 0.0
    return value.doubleValue()
  }

  def getMaxSlotValue(slotArray: ArrayBuffer[Int]): Int = {

    var maxSlot = 0
    var max = -1

    for (i <- 0 until slotArray.length) {

      if (slotArray(i) > max) {
        max = slotArray(i)
        maxSlot = i
      }

    }

    return maxSlot

  }

  /**
   * follow up campaign map
   * @param mailType
   * @return
   */
  def followUpCampaignMailTypes(mailType: Int): Int = {
    val followUpCampaignMap = collection.immutable.HashMap(
      CampaignCommon.SURF1_MAIL_TYPE -> CampaignCommon.SURF1_FOLLOW_UP_MAIL_TYPE,
      CampaignCommon.SURF2_MAIL_TYPE -> CampaignCommon.SURF2_FOLLOW_UP_MAIL_TYPE,
      CampaignCommon.SURF3_MAIL_TYPE -> CampaignCommon.SURF3_FOLLOW_UP_MAIL_TYPE,
      CampaignCommon.SURF6_MAIL_TYPE -> CampaignCommon.SURF6_FOLLOW_UP_MAIL_TYPE,
      CampaignCommon.CANCEL_RETARGET_MAIL_TYPE -> CampaignCommon.CANCEL_RETARGET_FOLLOW_UP_MAIL_TYPE,
      CampaignCommon.RETURN_RETARGET_MAIL_TYPE -> CampaignCommon.RETURN_RETARGET_FOLLOW_UP_MAIL_TYPE)

    return followUpCampaignMap.getOrElse(mailType, 0)
  }

  def nextPriceBand(priceBand: String): String = {
    if (priceBand == null || priceBand.equals("E")) {
      return priceBand
    } else {
      return (priceBand.charAt(0) + 1).toChar.toString
    }
  }

  // The last click date will be calculated based on click dates alone and the open dates are passed empty.
  // If the open dates are are null or empty, we need to check the values in click date to set the final open date  value.
  def latestEmailOpenDate(openDate: String, yesOpenDate: String, clickDate: String, yesClickDate: String): String = {
    var maxDateString: String = "2001-01-01 00:00:00"
    var maxDate: Date = TimeUtils.getDate(maxDateString, TimeConstants.DATE_TIME_FORMAT)
    var date: Date = null
    if (null != openDate && openDate.length > 0) {
      date = TimeUtils.getDate(openDate, TimeConstants.DATE_TIME_FORMAT)
    } else if (null != yesOpenDate && yesOpenDate.length > 0) {
      date = TimeUtils.getDate(yesOpenDate, TimeConstants.DATE_TIME_FORMAT)
    }
    if (date != null && date.after(maxDate)) {
      maxDateString = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT).format(date)
    } else {
      if (null != clickDate && clickDate.length > 0) {
        maxDate = TimeUtils.getDate(clickDate, TimeConstants.DATE_TIME_FORMAT)
      } else if (null != yesClickDate && yesClickDate.length > 0) {
        maxDate = TimeUtils.getDate(yesClickDate, TimeConstants.DATE_TIME_FORMAT)
      }
      maxDateString = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT).format(maxDate)
    }
    maxDateString
  }

  def size(a: mutable.MutableList[String]): Int = {
    if (a == null) return 0
    a.size
  }

  def nonBeauty(category: String, created_at: Timestamp): String = {
    //println(category,created_at)
    if (category == null || created_at == null) {
      return null
    }

    val NonBeautyCategory = scala.collection.mutable.HashMap.empty[String, Int]
    NonBeautyCategory += (
      "SUNGLASSES" -> 365,
      "WOMEN FOOTWEAR" -> 180,
      "KIDS APPAREL" -> 180,
      "WATCHES" -> 365,
      "FURNITURE" -> 365,
      "SPORTS EQUIPMENT" -> 180,
      "WOMEN APPAREL" -> 90,
      "HOME" -> 365,
      "MEN FOOTWEAR" -> 180,
      "MEN APPAREL" -> 180,
      "JEWELLERY" -> 180,
      "KIDS FOOTWEAR" -> 180,
      "BAGS" -> 90,
      "TOYS" -> 90
    )
    val lastPurchaseDay = NonBeautyCategory.getOrElse(category, null)

    if (lastPurchaseDay == null) {
      return null
    }

    // println(TimeUtils.daysFromToday(created_at.toString, TimeConstants.DATE_TIME_FORMAT),lastPurchaseDay.asInstanceOf[Int])
    if (TimeUtils.daysFromToday(created_at.toString, TimeConstants.DATE_TIME_FORMAT) == lastPurchaseDay.asInstanceOf[Int]) {
      return category
    }

    return null

  }

  def beauty(category: String, created_at: Timestamp): String = {
    //    println(category,created_at)
    if (category == null || created_at == null) {
      return null
    }

    val BeautyCategory = scala.collection.mutable.HashMap.empty[String, Int]
    BeautyCategory += (
      "BEAUTY" -> 90,
      "FRAGRANCE" -> 180
    )

    val lastPurchaseDay = BeautyCategory.getOrElse(category, null)

    if (lastPurchaseDay == null) {
      return null
    }
    // println(TimeUtils.daysFromToday(created_at.toString, TimeConstants.DATE_TIME_FORMAT),lastPurchaseDay.asInstanceOf[Int])

    if (TimeUtils.daysFromToday(created_at.toString, TimeConstants.DATE_TIME_FORMAT) == lastPurchaseDay.asInstanceOf[Int]) {
      return category
    }

    null

  }

  def lengthString(string: String): Int = {
    if (string == null)
      return 0

    string.length()
  }

  def acartNumberOfSkus(acartUrl: String): Int = {
    if (acartUrl == null)
      return 0

    val acartData = acartUrl.split("=")
    if (acartData.length > 1) {
      return acartData(0).split(",").length
    }
    0
  }

  def addMaskString(col: String, mask: String): String = {
    if (null == col || col.length == 0) {
      return ""
    }
    (mask + col + mask)
  }

  def modeFilter(created_at: Timestamp, incrDate: String, n: Int, modBase: Int): Boolean = {
    // calculate days since today
    val daysSince = TimeUtils.daysFromIncrDate(created_at.toString, incrDate + " " + TimeConstants.END_TIME, TimeConstants.DATE_TIME_FORMAT)
    if ((daysSince == 0) && (n == 0)) return false
    if (daysSince % modBase == n) true
    else false
  }

}

