package com.jabong.dap.model.ad4push

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.jabong.dap.common.{Spark, SharedSparkContext}
import com.jabong.dap.common.constants.variables.DevicesReactionsVariables
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.ad4push.schema.DevicesReactionsSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.TimestampType
import org.scalatest.FlatSpec
import com.jabong.dap.model.ad4push.variables.DevicesReactions
/**
 * Created by Kapil.Rajak on 13/7/15.
 */
class DevicesReactionsTest  extends FlatSpec with SharedSparkContext {

  "io : DataFrame" should "match with expected DF" in {
//    val full =JsonUtils.readFromJsonAbsPath(DataSets.DEVICE_REACTION_JSON_DIRECTORY, "fullSummary_full", DevicesReactionsSchema.deviceReaction)
//    //full.limit(10).write.json(DataSets.TEST_RESOURCES + "ad4push" + ".json")
//    val path = DataSets.DEVICE_REACTION_DF_DIRECTORY+"/"+"exportMessagesReactions_515_20150721.parquet"
//    //full.write.parquet(path)
//    val df = Spark.getSqlContext().read.parquet(path)

//    val (iPhone,android) = DevicesReactions.io("20150722")
//    iPhone.limit(10).write.json(DataSets.TEST_RESOURCES + "ad4pushI" + ".json")
//    android.limit(10).write.json(DataSets.TEST_RESOURCES + "ad4pushA" + ".json")
  }
  "dataFrameFromCsvPath: Data Frame" should "match with expected data" in {
    val path = DataSets.DEVICE_REACTION_CSV_DIRECTORY + "/" + "test.csv"
    val dfReaction = DevicesReactions.dataFrameFromCsvPath(path);
    //dfReaction.limit(10).write.json(DataSets.TEST_RESOURCES + "ad4push" + ".json")
    val dfExpected = JsonUtils.readFromJsonAbsPath(DataSets.DEVICE_REACTION_JSON_DIRECTORY, "testDF", DevicesReactionsSchema.dfFromCsv)
    assert(dfExpected.collect().toSet.equals(dfReaction.collect().toSet))
  }
  "reduce: dataFrame" should "match with expected data" in {
    val reduceInDF = JsonUtils.readFromJsonAbsPath(DataSets.DEVICE_REACTION_JSON_DIRECTORY, "reduceIn", DevicesReactionsSchema.dfFromCsv)
    val result = DevicesReactions.reduce(reduceInDF)
    val expectedResult = JsonUtils.readFromJsonAbsPath(DataSets.DEVICE_REACTION_JSON_DIRECTORY, "reduceOut", DevicesReactionsSchema.reducedDF)
    assert(result.collect().toSet.equals(expectedResult.collect().toSet))
  }

  "effectiveDFFull: DataFrame" should "match with expected data" in {
    val incremental = JsonUtils.readFromJsonAbsPath(DataSets.DEVICE_REACTION_JSON_DIRECTORY, "effectiveDFFull_incremental", DevicesReactionsSchema.dfFromCsv)
    val effective7 = JsonUtils.readFromJsonAbsPath(DataSets.DEVICE_REACTION_JSON_DIRECTORY, "effectiveDFFull_effective7", DevicesReactionsSchema.dfFromCsv)
    val effective15 = JsonUtils.readFromJsonAbsPath(DataSets.DEVICE_REACTION_JSON_DIRECTORY, "effectiveDFFull_effective15", DevicesReactionsSchema.dfFromCsv)
    val effective30 = JsonUtils.readFromJsonAbsPath(DataSets.DEVICE_REACTION_JSON_DIRECTORY, "effectiveDFFull_effective30", DevicesReactionsSchema.dfFromCsv)
    val effectiveDFFull = DevicesReactions.effectiveDFFull(DevicesReactions.reduce(incremental), DevicesReactions.reduce(effective7), DevicesReactions.reduce(effective15), DevicesReactions.reduce(effective30))
    //effectiveDFFull.limit(10).write.json(DataSets.TEST_RESOURCES + "ad4push" + ".json")
    val expectedDF = JsonUtils.readFromJsonAbsPath(DataSets.DEVICE_REACTION_JSON_DIRECTORY, "effectiveDFFull_result", DevicesReactionsSchema.effectiveDF)
    assert(expectedDF.collect().toSet.equals(effectiveDFFull.collect().toSet))
  }
  "fullSummary: DataFrame" should "match with expected data" in {
    val toDay = "20150712"
    val full = JsonUtils.readFromJsonAbsPath(DataSets.DEVICE_REACTION_JSON_DIRECTORY, "fullSummary_full", DevicesReactionsSchema.deviceReaction)
    val incrementalDF = JsonUtils.readFromJsonAbsPath(DataSets.DEVICE_REACTION_JSON_DIRECTORY, "fullSummary_incremental", DevicesReactionsSchema.dfFromCsv)
    val before7days = JsonUtils.readFromJsonAbsPath(DataSets.DEVICE_REACTION_JSON_DIRECTORY, "fullSummary_before7days", DevicesReactionsSchema.dfFromCsv)
    val before15days = JsonUtils.readFromJsonAbsPath(DataSets.DEVICE_REACTION_JSON_DIRECTORY, "fullSummary_before15days", DevicesReactionsSchema.dfFromCsv)
    val before30days = JsonUtils.readFromJsonAbsPath(DataSets.DEVICE_REACTION_JSON_DIRECTORY, "fullSummary_before30days", DevicesReactionsSchema.dfFromCsv)

    //incrementalDF = null
    val f_7_15_30 = DevicesReactions.fullSummary(null, toDay, full, null, null, null)
    assert(full.collect().toSet.equals(f_7_15_30.collect().toSet))

    val i_f_7_15_30 = DevicesReactions.fullSummary(incrementalDF, toDay, full, before7days, before15days, before30days)
    //i_f_7_15_30.limit(12).write.json(DataSets.TEST_RESOURCES + "ad4push" + ".json")
    val expected_i_f_7_15_30 = JsonUtils.readFromJsonAbsPath(DataSets.DEVICE_REACTION_JSON_DIRECTORY, "i_f_7_15_30", DevicesReactionsSchema.deviceReaction)
    //:TODO check test files-data need to be verified
    assert(i_f_7_15_30.collect().toSet.equals(expected_i_f_7_15_30.collect().toSet))
  }
}
