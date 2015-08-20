package com.jabong.dap.model.ad4push

import java.io.File

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.constants.variables.DevicesReactionsVariables._
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.DataSets._
import com.jabong.dap.model.ad4push.schema.DevicesReactionsSchema._
import com.jabong.dap.model.ad4push.variables.DevicesReactions
import org.scalatest.FlatSpec

import scala.collection.mutable.ListBuffer

/**
 * Created by Kapil.Rajak on 13/7/15.
 */
class DevicesReactionsTest extends FlatSpec with SharedSparkContext {

  "dfCorrectSchema" should "filter and give correct count" in {
    val incIStringSchema = DataReader.getDataFrame4mCsv(JsonUtils.TEST_RESOURCES, DataSets.AD4PUSH, DataSets.CSV, DataSets.DAILY_MODE, "2015/07/27", "exportMessagesReactions_517_20150727.csv", "true", ",")
    val incI = DevicesReactions.dfCorrectSchema(incIStringSchema)
    assert(incI.count() == 97)
  }

  "reduce" should "match with the expected Data" in {
    val inputDF = JsonUtils.readFromJson(DataSets.AD4PUSH, "CorrectedSchema20150727", dfFromCsv)
    val result = DevicesReactions.reduce(inputDF)
    assert(result.count() == 91)
  }

  "reduce: dataFrame" should "match with expected data" in {
    val reduceInDF = JsonUtils.readFromJson(AD4PUSH, "reduceIn", dfFromCsv)
    val result = DevicesReactions.reduce(reduceInDF)
    val expectedResult = JsonUtils.readFromJson(AD4PUSH, "reduceOut", reducedDF)
    assert(result.collect().toSet.equals(expectedResult.collect().toSet))
  }

  "effectiveDFFull" should "match with expected data" in {
    val incremental = JsonUtils.readFromJson(AD4PUSH, "Reduced20150727", reducedDF)
    val reduced7 = JsonUtils.readFromJson(AD4PUSH, "Incr20150720", reducedDF)
    val reduced15 = JsonUtils.readFromJson(AD4PUSH, "Incr20150712", reducedDF)
    val reduced30 = null
    val effectiveDFFull = DevicesReactions.effectiveDFFull(incremental, reduced7, reduced15, reduced30)
    assert(effectiveDFFull.count() == 25406)
  }

  "effectiveDFFull: DataFrame" should "match with expected data" in {
    val incremental = JsonUtils.readFromJson(AD4PUSH, "effectiveDFFull_incremental", dfFromCsv)
    val effective7 = JsonUtils.readFromJson(AD4PUSH, "effectiveDFFull_effective7", reducedDF)
    val effective15 = JsonUtils.readFromJson(AD4PUSH, "effectiveDFFull_effective15", reducedDF)
    val effective30 = JsonUtils.readFromJson(AD4PUSH, "effectiveDFFull_effective30", reducedDF)
    val effectiveDFFull = DevicesReactions.effectiveDFFull(DevicesReactions.reduce(incremental), DevicesReactions.reduce(effective7), DevicesReactions.reduce(effective15), DevicesReactions.reduce(effective30))
    val expectedDF = JsonUtils.readFromJson(AD4PUSH, "effectiveDFFull_result", effectiveDF)
    assert(expectedDF.collect().toSet.equals(effectiveDFFull.collect().toSet))
  }

  "Validation" should "be done" in {
    val file = JsonUtils.TEST_RESOURCES + File.separator + AD4PUSH + File.separator + "fullSummary_full" + ".json"
    val lines = scala.io.Source.fromFile(file).mkString
    val arrayJson = lines.split("\n")
    var listMaps: ListBuffer[Map[String, Any]] = ListBuffer()
    for (json <- arrayJson) {
      val map = json.trim.substring(1, json.length - 1)
        .split(",")
        .map(_.split(":"))
        .map { case Array(k, v) => (k.dropRight(1).drop(1), v filterNot ("\"" contains _)) }
        .toMap
      listMaps += map
    }
  }

  "fullSummary: DataFrame" should "match with expected data" in {
    val toDay = "2015/07/12"
    val full = JsonUtils.readFromJson(AD4PUSH, "fullSummary_full", deviceReaction)
    val incrementalDF = DevicesReactions.reduce(JsonUtils.readFromJson(AD4PUSH, "fullSummary_incremental", dfFromCsv))
    val before7days = JsonUtils.readFromJson(AD4PUSH, "fullSummary_before7days", dfFromCsv)
    val before15days = JsonUtils.readFromJson(AD4PUSH, "fullSummary_before15days", dfFromCsv)
    val before30days = JsonUtils.readFromJson(AD4PUSH, "fullSummary_before30days", dfFromCsv)

    //incrementalDF = null
    val (f_7_15_30, _) = DevicesReactions.fullSummary(null, toDay, full, null, null, null)
    assert(full.collect().toSet.equals(f_7_15_30.collect().toSet))

    val (i_f_7_15_30, _) = DevicesReactions.fullSummary(incrementalDF, toDay, full, before7days, before15days, before30days)
    val expected_i_f_7_15_30 = JsonUtils.readFromJson(AD4PUSH, "i_f_7_15_30", deviceReaction)
    assert(i_f_7_15_30.collect().toSet.equals(expected_i_f_7_15_30.collect().toSet))

    //validating data with JSON read
  }

  "Robustness Testing" should "be done to verify correctness" in {
    // files are taken to verify from the test: fullSummary
    val full = JsonUtils.jsonsFile2ArrayOfMap(AD4PUSH, "fullSummary_full")
    val before7days = JsonUtils.jsonsFile2ArrayOfMap(AD4PUSH, "fullSummary_before7days")
    val before15days = JsonUtils.jsonsFile2ArrayOfMap(AD4PUSH, "fullSummary_before15days")
    val before30days = JsonUtils.jsonsFile2ArrayOfMap(AD4PUSH, "fullSummary_before30days")

    val result = JsonUtils.jsonsFile2ArrayOfMap(AD4PUSH, "i_f_7_15_30")

    for (map <- result) {
      var custId = if (map.contains(CUSTOMER_ID)) map(CUSTOMER_ID) else null
      var devId = if (map.contains(DEVICE_ID)) map(DEVICE_ID) else null
      var lastClickD = if (map.contains(LAST_CLICK_DATE)) map(LAST_CLICK_DATE) else null
      var click7 = if (map.contains(CLICK_7)) map(CLICK_7).toInt else 0
      var click15 = if (map.contains(CLICK_15)) map(CLICK_15).toInt else 0
      var click30 = if (map.contains(CLICK_30)) map(CLICK_30).toInt else 0
      var clickLife = if (map.contains(CLICK_LIFETIME)) map(CLICK_LIFETIME).toInt else null
      var clickMon = if (map.contains(CLICK_MONDAY)) map(CLICK_MONDAY).toInt else 0
      var clickTue = if (map.contains(CLICK_TUESDAY)) map(CLICK_TUESDAY).toInt else 0
      var clickWed = if (map.contains(CLICK_WEDNESDAY)) map(CLICK_WEDNESDAY).toInt else 0
      var clickThu = if (map.contains(CLICK_THURSDAY)) map(CLICK_THURSDAY).toInt else 0
      var clickFri = if (map.contains(CLICK_FRIDAY)) map(CLICK_FRIDAY).toInt else 0
      var clickSat = if (map.contains(CLICK_SATURDAY)) map(CLICK_SATURDAY).toInt else 0
      var clickSun = if (map.contains(CLICK_SUNDAY)) map(CLICK_SUNDAY).toInt else 0
      var click2wice = if (map.contains(CLICKED_TWICE)) map(CLICKED_TWICE).toInt else 0
      var mostClickDay = if (map.contains(MOST_CLICK_DAY)) map(MOST_CLICK_DAY) else null

      logger.info("Testing row with device_id: " + devId + ", customer_id: " + custId)

      val shouldBe = maxClickDay(clickMon, clickTue, clickWed, clickThu, clickFri, clickSat, clickSun)
      assert(mostClickDay.toLowerCase.equals(shouldBe))
      //:TODO add consistent inputs/files to get correct result
      //assert(clickLife.equals(clickMon+clickTue+clickWed+clickThu+clickFri+clickSat+clickSun))
      //assert(click7 <= click15 && click15 <= click30)
    }
  }

  def maxClickDay(clicks: Int*): String = {
    val days = List("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
    var max = clicks(0)
    var maxIndex: Int = 0
    for (i <- 1 until clicks.length) {
      if (max < clicks(i)) {
        max = clicks(i)
        maxIndex = i
      }
    }
    days(maxIndex).toLowerCase
  }
}
