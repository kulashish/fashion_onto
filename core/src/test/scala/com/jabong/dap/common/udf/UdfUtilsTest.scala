package com.jabong.dap.common.udf

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.scalatest.FlatSpec

/**
 * Created by raghu on 3/7/15.
 */
class UdfUtilsTest extends FlatSpec {

  //===============================getMin()=============================================================================
  "getMin(): timestamp t1 and t2 value " should "be null" in {

    val t1 = null

    val t2 = null

    val result = UdfUtils.getMin(t1, t2)

    assert(result == null)

  }

  "getMin(): timestamp t1" should "be null" in {

    val t1 = null

    val t2 = Timestamp.valueOf("2015-04-30 00:05:07.0")

    val result = UdfUtils.getMin(t1, t2)

    assert(result.compareTo(t2) == 0)

  }

  "getMin(): timestamp t2" should "be null" in {

    val t1 = Timestamp.valueOf("2015-04-30 00:05:07.0")

    val t2 = null

    val result = UdfUtils.getMin(t1, t2)

    assert(result.compareTo(t1) == 0)

  }

  "getMin(): return timestamp " should "t1" in {

    val t1 = Timestamp.valueOf("2015-04-30 00:05:07.0")

    val t2 = Timestamp.valueOf("2015-04-30 00:05:09.0")

    val result = UdfUtils.getMin(t1, t2)

    assert(result.compareTo(t1) >= 0)

  }

  "getMin(): return timestamp " should "t2" in {

    val t1 = Timestamp.valueOf("2015-04-30 00:05:09.0")

    val t2 = Timestamp.valueOf("2015-04-30 00:05:07.0")

    val result = UdfUtils.getMin(t1, t2)

    assert(result.compareTo(t2) >= 0)

  }

  //===============================getMax()=============================================================================
  "getMax(): timestamp t1 and t2 value " should "be null" in {

    val t1 = null

    val t2 = null

    val result = UdfUtils.getMax(t1, t2)

    assert(result == null)

  }

  "getMax(): timestamp t1" should "be null" in {

    val t1 = null

    val t2 = Timestamp.valueOf("2015-04-30 00:05:07.0")

    val result = UdfUtils.getMax(t1, t2)

    assert(result.compareTo(t2) == 0)

  }

  "getMax(): timestamp t2" should "be null" in {

    val t1 = Timestamp.valueOf("2015-04-30 00:05:07.0")

    val t2 = null

    val result = UdfUtils.getMax(t1, t2)

    assert(result.compareTo(t1) == 0)

  }

  "getMax(): return timestamp " should "t2" in {

    val t1 = Timestamp.valueOf("2015-04-30 00:05:07.0")

    val t2 = Timestamp.valueOf("2015-04-30 00:05:09.0")

    val result = UdfUtils.getMax(t1, t2)

    assert(result.compareTo(t2) == 0)

  }

  "getMax(): return timestamp " should "t1" in {

    val t1 = Timestamp.valueOf("2015-04-30 00:05:09.0")

    val t2 = Timestamp.valueOf("2015-04-30 00:05:07.0")

    val result = UdfUtils.getMax(t1, t2)

    assert(result.compareTo(t1) == 0)

  }

  //===============================getSimpleSkuFromExtraData()==========================================================
  "getSimpleSkuFromExtraData: simple_sku" should "LA625BG58FVTINDFAS-3949337" in {

    val extraData = "{\"simple_sku\":\"LA625BG58FVTINDFAS-3949337\",\"price\":1599,\"all_colors\":\"LA625BG58FVTINDFAS\",\"sel_size_qty\":\"1\",\"id_catalog_config\":\"1251841\",\"all_simples\":{\"LA625BG58FVTINDFAS-3949337\":\"1\"}}"

    val result = UdfUtils.getSimpleSkuFromExtraData(extraData)

    assert(result == "LA625BG58FVTINDFAS-3949337")

  }

  "getSimpleSkuFromExtraData: extraData value" should "null" in {

    val result = UdfUtils.getSimpleSkuFromExtraData(null)

    assert(result == null)

  }

  "getSimpleSkuFromExtraData: extraData value" should "Array" in {

    val result = UdfUtils.getSimpleSkuFromExtraData("Array")

    assert(result == null)

  }

  "getSimpleSkuFromExtraData: simple_sku" should "true" in {

    val extraData = "{\"simple_sku\":\"true\",\"price\":1599,\"all_colors\":\"LA625BG58FVTINDFAS\",\"sel_size_qty\":\"1\",\"id_catalog_config\":\"1251841\",\"all_simples\":{\"LA625BG58FVTINDFAS-3949337\":\"1\"}}"

    val result = UdfUtils.getSimpleSkuFromExtraData(extraData)

    assert(result == null)

  }

  //===============================getPriceFromExtraData()==============================================================
  "getPriceFromExtraData: price" should "1599" in {

    val extraData = "{\"simple_sku\":\"LA625BG58FVTINDFAS-3949337\",\"price\":1599,\"all_colors\":\"LA625BG58FVTINDFAS\",\"sel_size_qty\":\"1\",\"id_catalog_config\":\"1251841\",\"all_simples\":{\"LA625BG58FVTINDFAS-3949337\":\"1\"}}"

    val result = UdfUtils.getPriceFromExtraData(extraData)

    assert(result == 1599)

  }

  "getPriceFromExtraData: extraData value" should "null" in {

    val result = UdfUtils.getPriceFromExtraData(null)

    assert(result == 0)

  }

  "getPriceFromExtraData: extraData value" should "Array" in {

    val result = UdfUtils.getPriceFromExtraData("Array")

    assert(result == 0)

  }

  //===============================getskuFromSimpleSku()================================================================
  "getskuFromSimpleSku: simpleSku value" should "null" in {

    val result = UdfUtils.getskuFromSimpleSku(null)

    assert(result == null)

  }

  "getskuFromSimpleSku: simpleSku value" should "LA625BG58FVTINDFAS-3949337" in {

    val result = UdfUtils.getskuFromSimpleSku("LA625BG58FVTINDFAS-3949337")

    assert(result == "LA625BG58FVTINDFAS")

  }

  "getskuFromSimpleSku: simpleSku value" should "LA625BG58FVTINDFAS" in {

    val result = UdfUtils.getskuFromSimpleSku("LA625BG58FVTINDFAS")

    assert(result == "LA625BG58FVTINDFAS")

  }

  //===============================getYYYYmmDD()========================================================================
  "getYYYYmmDD: Timestamp value" should "null" in {
    val ts: String = null

    val result = UdfUtils.getYYYYmmDD(ts)

    assert(result == null)

  }

  "getYYYYmmDD: Timestamp value" should "2015-07-13 00:02:22.0" in {

    val ts = Timestamp.valueOf("2015-07-13 00:02:22.0")

    val result = UdfUtils.getYYYYmmDD(ts)

    assert(result == Timestamp.valueOf("2015-07-13 00:00:00.0"))

  }

  //===============================getAge()=============================================================================
  "getAge: age value" should "null" in {

    val result = UdfUtils.getAge(null)

    assert(result == 0)

  }

  "getAge: age value" should "2010-07-13 00:02:22.0" in {

    val ts = Timestamp.valueOf("2010-07-13 00:02:22.0")

    val result = UdfUtils.getAge(ts)

    assert(result == 5)

  }
  //
  //  //===============================getLatest()============================================================================
  //  "getLatest(): timestamp t1 and t2 value " should "be null" in {
  //
  //    val t1 = null
  //
  //    val t2 = null
  //
  //    val result = UdfUtils.getLatest(t1, t2)
  //
  //    assert(result == t1)
  //
  //  }

  "getLatest(): timestamp t1" should "be null" in {

    val t1 = null

    val t2 = Timestamp.valueOf("2015-04-30 00:05:07.0")

    val result = UdfUtils.getLatest(t1, t2)

    assert(result == t2)

  }

  "getLatest(): timestamp t2" should "be null" in {

    val t1 = Timestamp.valueOf("2015-04-30 00:05:07.0")

    val t2 = null

    val result = UdfUtils.getLatest(t1, t2)

    assert(result == t1)

  }

  "getLatest(): return timestamp " should "t2" in {

    val t1 = Timestamp.valueOf("2015-04-30 00:05:07.0")

    val t2 = Timestamp.valueOf("2015-04-30 00:05:09.0")

    val result = UdfUtils.getLatest(t1, t2)

    assert(result == t2)

  }

  //===============================getMergeSlots()======================================================================
  "getMergeSlots(): oldSlot and newSlot value " should "be null" in {

    val oldSlot = null

    val newSlot = null

    val result = UdfUtils.getMergeSlots(oldSlot, newSlot)

    assert(result == null)

  }

  "getMergeSlots(): oldSlot value" should "be null" in {

    val oldSlot = null

    val newSlot = "0!0!0!0!0!1!0!0!0!0!0!0"

    val result = UdfUtils.getMergeSlots(oldSlot, newSlot)

    assert(result == newSlot)

  }

  "getMergeSlots(): newSlot value" should "be null" in {

    val oldSlot = "0!0!0!0!0!1!0!0!0!0!0!0"

    val newSlot = null

    val result = UdfUtils.getMergeSlots(oldSlot, newSlot)

    assert(result == oldSlot)

  }

  "getMergeSlots(): return slot value " should "0!0!0!0!0!2!0!5!0!4!0!0" in {

    val oldSlot = "0!0!0!0!0!1!0!0!0!4!0!0"

    val newSlot = "0!0!0!0!0!1!0!5!0!0!0!0"

    val result = UdfUtils.getMergeSlots(oldSlot, newSlot)

    assert(result == "0!0!0!0!0!2!0!5!0!4!0!0")

  }

  //===============================getMaxSlot(oldSlot, newSlot, oldPreferredSlot)=======================================
  "getMaxSlot(oldSlot, newSlot, oldPreferredSlot): oldSlot and newSlot value " should "be null" in {

    val oldSlot = null

    val newSlot = null

    val oldPreferredSlot = 0

    val result = UdfUtils.getMaxSlot(oldSlot, newSlot, oldPreferredSlot)

    assert(result == 0)

  }

  "getMaxSlot(oldSlot, newSlot, oldPreferredSlot): oldSlot value" should "be null" in {

    val oldSlot = null

    val newSlot = "0!0!0!0!0!1!0!0!0!0!0!0"

    val oldPreferredSlot = 0

    val result = UdfUtils.getMaxSlot(oldSlot, newSlot, oldPreferredSlot)

    assert(result == 6)

  }

  "getMaxSlot(oldSlot, newSlot, oldPreferredSlot): newSlot value" should "be null" in {

    val oldSlot = "0!0!0!0!0!1!0!0!0!0!0!0"

    val newSlot = null

    val oldPreferredSlot = 7

    val result = UdfUtils.getMaxSlot(oldSlot, newSlot, oldPreferredSlot)

    assert(result == 7)

  }

  "getMaxSlot(oldSlot, newSlot, oldPreferredSlot): return slot value " should "0!0!0!0!0!2!0!5!0!4!0!0" in {

    val oldSlot = "0!0!0!0!0!1!0!0!0!4!0!0"

    val newSlot = "0!0!0!0!0!1!0!5!0!0!0!0"

    val oldPreferredSlot = 0

    val result = UdfUtils.getMaxSlot(oldSlot, newSlot, oldPreferredSlot)

    assert(result == 8)

  }

  //===============================getMaxSlot(slots: Any)=========================================================
  "getMaxSlot(slot): slot value " should "be null" in {

    val slot = null

    val result = UdfUtils.getMaxSlot(slot)

    assert(result == 0)

  }

  "getMaxSlot(slot): slot value " should "0!0!0!0!0!1!0!5!0!0!0!0" in {

    val slot = "0!0!0!0!0!1!0!5!0!0!0!0"

    val result = UdfUtils.getMaxSlot(slot)

    assert(result == 8)

  }

  //===============================getDistinctSku=========================================================
  "getDistinctSku(): skuArray value " should "be null" in {

    val skuArray = null

    val result = UdfUtils.getDistinctList(skuArray)

    assert(result == null)

  }

  //  "getDistinctSku(): skuArray value " should "not be null" in {
  //
  //    val skuArray = Array("a", "b", "a", "c", "c", "d", "d")
  //
  //    val result = UdfUtils.getDistinctSku(skuArray.toList)
  //
  //    assert(result.length == 4)
  //
  //  }

  //===============================getRepeatedSku=========================================================
  "getRepeatedSku(): skuArray value " should "be null" in {

    val skuArray = null

    val result = UdfUtils.getRepeatedSku(skuArray)

    assert(result == null)

  }

  //  "getRepeatedSku(): skuArray value " should "not be null" in {
  //
  //    val skuArray = Array("a", "b", "a", "c", "c", "d", "d")
  //
  //    val result = UdfUtils.getRepeatedSku(skuArray.toBuffer)
  //
  //    assert(result.length == 3)
  //
  //  }

  "getMaxClickDayName" should "be Sunday" in {
    val day = UdfUtils.getMaxClickDayName(1, 2, 3, 4, 5, 6, 7)
    assert(day.equals("Sunday"))
  }

  //===============================removeAllZero=========================================================
  "removeAllZero" should "be empty String" in {
    val str = UdfUtils.removeAllZero("000000")
    assert(str.equals(""))
  }

  "removeAllZero" should "be same as input string" in {
    val str = UdfUtils.removeAllZero("DE683C47-06E5-4817-BE06-066DEEBA8E4D")
    assert(str.equals("DE683C47-06E5-4817-BE06-066DEEBA8E4D"))
  }

  //===============================allZero2Null=========================================================

  "allZero2Null" should "be null String 1" in {
    val str = UdfUtils.allZero2Null("000000")
    assert(str == null)
  }

  "allZero2Null" should "be null String 2" in {
    val str = UdfUtils.allZero2Null("")
    assert(str == null)
  }

  "allZero2Null" should "be null String 3" in {
    val str = UdfUtils.allZero2Null("  ")
    assert(str == null)
  }

  "allZero2Null" should "be null String 4" in {
    val in: String = null
    val str = UdfUtils.allZero2Null(in)
    assert(str == null)
  }

  "allZero2Null" should "be same as input string" in {
    val str = UdfUtils.allZero2Null("DE683C47-06E5-4817-BE06-066DEEBA8E4D")
    assert(str.equals("DE683C47-06E5-4817-BE06-066DEEBA8E4D"))
  }

  //===============================getToLong=========================================================
  "getToLong():String value " should "be null" in {

    val str = null

    val result = UdfUtils.getToLong(str)

    assert(result == 0)

  }

  "getToLong():String value " should "be -ve value" in {

    val str = "-3"

    val result = UdfUtils.getToLong(str)

    assert(result == -3)

  }

  "getToLong():String value " should "be +ve value" in {

    val str = "3"

    val result = UdfUtils.getToLong(str)

    assert(result == 3)

  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //Name of variable: EMAIL_OPT_IN_STATUS
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  "getEmailOptInStatus: getStatusValue " should "O" in {

    val result = UdfUtils.getEmailOptInStatus(null, null)

    assert(result == "O")

  }

  "getEmailOptInStatus: getStatusValue " should "I" in {

    val row = Row("", "subscribed")

    val result = UdfUtils.getEmailOptInStatus("1", "subscribed")

    assert(result == "I")

  }

  "getEmailOptInStatus: getStatusValue " should "U" in {

    val row = Row("", "unsubscribed")

    val result = UdfUtils.getEmailOptInStatus("1", "unsubscribed")

    assert(result == "U")

  }

}
