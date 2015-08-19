package com.jabong.dap.export

import java.io.File

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.constants.SkuDataConst._
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.ad4push.schema.DevicesReactionsSchema._
import org.apache.spark.sql.types.{ LongType, StringType, StructField, StructType }
import org.scalatest.{ Matchers, FlatSpec }

/**
 * Created by Kapil.Rajak on 18/8/15.
 */
class SkuDataTest extends FlatSpec with SharedSparkContext with Matchers {
  //val TEST_RESOURCES = "src" + File.separator + "test" + File.separator + "resources"
  val yesterday = "2015/08/08"
  val schema = StructType(Array(
    StructField(PRODUCTSKU, StringType, true),
    StructField(DOMAIN, StringType, true)
  ))

  val schemaE = StructType(Array(
    StructField(DATE, StringType, false),
    StructField(PRODUCTSKU, StringType, true),
    StructField(PAGEVISIT_DESKTOP, LongType, true),
    StructField(PAGEVISIT_MOBILEWEB, LongType, true),
    StructField(PAGEVISIT_IOS, LongType, true),
    StructField(PAGEVISIT_ANDROID, LongType, true),
    StructField(PAGEVISIT_WINDOWS, LongType, true)
  ))

  "skuBasedProcess" should "behave as expected" in {
    val inputDF = JsonUtils.readFromJson(FOLDER, "skuData", schema)
    inputDF.printSchema()
    val result = SkuData.skuBasedProcess(inputDF, yesterday)
    result.printSchema()

    val expectedResult = JsonUtils.readFromJson(FOLDER, "skuDataProcessed", schemaE)
    assert(expectedResult.collect().toSet.equals(result.collect().toSet))
    //result.limit(10).write.json(TEST_RESOURCES + "ad4push" + ".json")
  }
}
