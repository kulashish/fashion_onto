package com.jabong.dap.model.ad4push

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.ad4push.schema.DevicesReactionsSchema
import com.jabong.dap.model.ad4push.variables.DevicesReactions
import org.scalatest.FlatSpec

/**
 * Created by Kapil.Rajak on 13/7/15.
 */
class DevicesReactionsTest extends FlatSpec with SharedSparkContext {

  "io : DataFrame" should "match with expected DF" in {
    //    val full =JsonUtils.readFromJson(DataSets.DEVICE_REACTION_JSON_DIRECTORY, "fullSummary_full", DevicesReactionsSchema.deviceReaction)
    //    //full.limit(10).write.json(DataSets.TEST_RESOURCES + "ad4push" + ".json")
    //    val path = DataSets.DEVICE_REACTION_DF_DIRECTORY+"/"+"exportMessagesReactions_515_20150721.parquet"
    //    //full.write.parquet(path)
    //    val df = Spark.getSqlContext().read.parquet(path)

    //    val (iPhone,android) = DevicesReactions.io("20150722")
    //    iPhone.limit(10).write.json(DataSets.TEST_RESOURCES + "ad4pushI" + ".json")
    //    android.limit(10).write.json(DataSets.TEST_RESOURCES + "ad4pushA" + ".json")
  }

  "getDataFrame4mCsv: Data Frame" should "match with expected data" in {
    val dfReaction = DataReader.getDataFrame4mCsv(DataSets.TEST_RESOURCES, DataSets.AD4PUSH, DataSets.CSV, DataSets.DAILY_MODE, "2015/07/22", "true", ",")
    //dfReaction.limit(10).write.json(DataSets.TEST_RESOURCES + "ad4push" + ".json")
    val dfExpected = JsonUtils.readFromJson(DataSets.AD4PUSH, "testDF", DevicesReactionsSchema.dfFromCsv)
    assert(dfExpected.collect().toSet.equals(dfReaction.collect().toSet))
  }

  "reduce: dataFrame" should "match with expected data" in {
    val reduceInDF = JsonUtils.readFromJson(DataSets.AD4PUSH, "reduceIn", DevicesReactionsSchema.dfFromCsv)
    val result = DevicesReactions.reduce(reduceInDF)
    val expectedResult = JsonUtils.readFromJson(DataSets.AD4PUSH, "reduceOut", DevicesReactionsSchema.reducedDF)
    assert(result.collect().toSet.equals(expectedResult.collect().toSet))
  }

  "effectiveDFFull: DataFrame" should "match with expected data" in {
    val incremental = JsonUtils.readFromJson(DataSets.AD4PUSH, "effectiveDFFull_incremental", DevicesReactionsSchema.dfFromCsv)
    val effective7 = JsonUtils.readFromJson(DataSets.AD4PUSH, "effectiveDFFull_effective7", DevicesReactionsSchema.dfFromCsv)
    val effective15 = JsonUtils.readFromJson(DataSets.AD4PUSH, "effectiveDFFull_effective15", DevicesReactionsSchema.dfFromCsv)
    val effective30 = JsonUtils.readFromJson(DataSets.AD4PUSH, "effectiveDFFull_effective30", DevicesReactionsSchema.dfFromCsv)
    val effectiveDFFull = DevicesReactions.effectiveDFFull(DevicesReactions.reduce(incremental), DevicesReactions.reduce(effective7), DevicesReactions.reduce(effective15), DevicesReactions.reduce(effective30))
    //effectiveDFFull.limit(10).write.json(DataSets.TEST_RESOURCES + "ad4push" + ".json")
    val expectedDF = JsonUtils.readFromJson(DataSets.AD4PUSH, "effectiveDFFull_result", DevicesReactionsSchema.effectiveDF)
    assert(expectedDF.collect().toSet.equals(effectiveDFFull.collect().toSet))
  }
  "fullSummary: DataFrame" should "match with expected data" in {
    val toDay = "20150712"
    val full = JsonUtils.readFromJson(DataSets.AD4PUSH, "fullSummary_full", DevicesReactionsSchema.deviceReaction)
    val incrementalDF = JsonUtils.readFromJson(DataSets.AD4PUSH, "fullSummary_incremental", DevicesReactionsSchema.dfFromCsv)
    val before7days = JsonUtils.readFromJson(DataSets.AD4PUSH, "fullSummary_before7days", DevicesReactionsSchema.dfFromCsv)
    val before15days = JsonUtils.readFromJson(DataSets.AD4PUSH, "fullSummary_before15days", DevicesReactionsSchema.dfFromCsv)
    val before30days = JsonUtils.readFromJson(DataSets.AD4PUSH, "fullSummary_before30days", DevicesReactionsSchema.dfFromCsv)

    //incrementalDF = null
    val (f_7_15_30, _) = DevicesReactions.fullSummary(null, toDay, full, null, null, null)
    assert(full.collect().toSet.equals(f_7_15_30.collect().toSet))

    val (i_f_7_15_30, _) = DevicesReactions.fullSummary(incrementalDF, toDay, full, before7days, before15days, before30days)
    //i_f_7_15_30.limit(12).write.json(DataSets.TEST_RESOURCES + "ad4push" + ".json")
    val expected_i_f_7_15_30 = JsonUtils.readFromJson(DataSets.AD4PUSH, "i_f_7_15_30", DevicesReactionsSchema.deviceReaction)
    //:TODO check test files-data need to be verified
    assert(i_f_7_15_30.collect().toSet.equals(expected_i_f_7_15_30.collect().toSet))
  }
}
