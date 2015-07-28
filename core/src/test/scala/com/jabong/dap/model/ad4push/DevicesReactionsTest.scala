package com.jabong.dap.model.ad4push

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets._
import com.jabong.dap.model.ad4push.schema.DevicesReactionsSchema._
import com.jabong.dap.model.ad4push.variables.DevicesReactions
import org.scalatest.FlatSpec
/**
 * Created by Kapil.Rajak on 13/7/15.
 */
class DevicesReactionsTest extends FlatSpec with SharedSparkContext {

  "customerResponse : DataFrame" should "match with expected DF" in {
    //DevicesReactions.customerResponse("20150722", DAILY_MODE)
  }

  "reduce: dataFrame" should "match with expected data" in {
    val reduceInDF = JsonUtils.readFromJson(AD4PUSH, "reduceIn", dfFromCsv)
    val result = DevicesReactions.reduce(reduceInDF)
    val expectedResult = JsonUtils.readFromJson(AD4PUSH, "reduceOut", reducedDF)
    assert(result.collect().toSet.equals(expectedResult.collect().toSet))
    //result.limit(10).write.json(TEST_RESOURCES + "ad4push" + ".json")
  }

  "effectiveDFFull: DataFrame" should "match with expected data" in {
    val incremental = JsonUtils.readFromJson(AD4PUSH, "effectiveDFFull_incremental", dfFromCsv)
    val effective7 = JsonUtils.readFromJson(AD4PUSH, "effectiveDFFull_effective7", dfFromCsv)
    val effective15 = JsonUtils.readFromJson(AD4PUSH, "effectiveDFFull_effective15", dfFromCsv)
    val effective30 = JsonUtils.readFromJson(AD4PUSH, "effectiveDFFull_effective30", dfFromCsv)
    val effectiveDFFull = DevicesReactions.effectiveDFFull(DevicesReactions.reduce(incremental), DevicesReactions.reduce(effective7), DevicesReactions.reduce(effective15), DevicesReactions.reduce(effective30))
    //effectiveDFFull.limit(20).write.json(TEST_RESOURCES + "ad4push" + ".json")
    val expectedDF = JsonUtils.readFromJson(AD4PUSH, "effectiveDFFull_result", effectiveDF)
    assert(expectedDF.collect().toSet.equals(effectiveDFFull.collect().toSet))
  }
  "fullSummary: DataFrame" should "match with expected data" in {
    val toDay = "2015/07/12"
    val full = JsonUtils.readFromJson(AD4PUSH, "fullSummary_full", deviceReaction)
    val incrementalDF = JsonUtils.readFromJson(AD4PUSH, "fullSummary_incremental", dfFromCsv)
    val before7days = JsonUtils.readFromJson(AD4PUSH, "fullSummary_before7days", dfFromCsv)
    val before15days = JsonUtils.readFromJson(AD4PUSH, "fullSummary_before15days", dfFromCsv)
    val before30days = JsonUtils.readFromJson(AD4PUSH, "fullSummary_before30days", dfFromCsv)

    //incrementalDF = null
    val (f_7_15_30, _) = DevicesReactions.fullSummary(null, toDay, full, null, null, null)
    // f_7_15_30.limit(12).write.json(TEST_RESOURCES + "ad4pushF" + ".json")
    assert(full.collect().toSet.equals(f_7_15_30.collect().toSet))

    val (i_f_7_15_30, _) = DevicesReactions.fullSummary(incrementalDF, toDay, full, before7days, before15days, before30days)
    // i_f_7_15_30.limit(30).write.json(TEST_RESOURCES + "ad4push" + ".json")
    val expected_i_f_7_15_30 = JsonUtils.readFromJson(AD4PUSH, "i_f_7_15_30", deviceReaction)
    //:TODO check test files-data need to be verified
    assert(i_f_7_15_30.collect().toSet.equals(expected_i_f_7_15_30.collect().toSet))
  }
}
