package com.jabong.dap.common.udf

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{ Date }

import com.jabong.dap.common.ArrayUtils
import com.jabong.dap.common.time.{ Constants, TimeUtils }
import net.liftweb.json._
import org.codehaus.jettison.json.JSONArray

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
        if (value > max) { maxSlot = slot; max = value };
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
   * This will return Timestamp into YYYYMMDD format
   * @param t1
   * @return
   */
  def getYYYYmmDD(t1: Timestamp): Timestamp = {

    if (t1 == null) {
      return null
    }

    val time = t1.toString()

    return Timestamp.valueOf(time.substring(0, time.indexOf(" ") + 1) + Constants.START_TIME_MS)
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

    val jsonExtraData = parse(extraData)

    val simple_sku = compact(render(jsonExtraData \ "simple_sku")).replaceAll("^\"|\"$", "")

    if (simple_sku.length() < 10)
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

    val jsonExtraData = parse(extraData)

    val price = compact(render(jsonExtraData \ "price")).replaceAll("^\"|\"$", "")

    return price.toDouble

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

    return simpleSku.substring(0, simpleSku.lastIndexOf('-'))
  }

}
