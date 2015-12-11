package com.jabong.dap.data.storage.merge.common

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.variables.CustomerVariables
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, LongType, StructField, StructType}
import org.scalatest.FlatSpec

class MergeUtilsTest extends FlatSpec with SharedSparkContext {

  @transient var df1: DataFrame = _
  @transient var df2: DataFrame = _

  @transient var oldDF: DataFrame = _
  @transient var newDF: DataFrame = _

  override def beforeAll() {
    super.beforeAll()

    df1 = JsonUtils.readFromJson("common/merge", "1")
    df2 = JsonUtils.readFromJson("common/merge", "2")
    oldDF = JsonUtils.readFromJson("common/merge", "mergeOld")
    newDF = JsonUtils.readFromJson("common/merge", "mergeNew")
  }

  "A Merged DF" should "have size 4" in {
    var mergedDF = MergeUtils.InsertUpdateMerge(df1, df2, "name")
    mergedDF.collect.foreach(println)
    assert(mergedDF.collect.size == 4)
  }

  "A Merged DF" should "have size 2" in {
    var mergedDF = MergeUtils.InsertUpdateMerge(null, df2, "name")
    mergedDF.collect.foreach(println)
    assert(mergedDF.collect.size == 2)
  }

  "A Merged DF" should "have size 3" in {
    var mergedDF = MergeUtils.InsertUpdateMerge(df1, null, "name")
    mergedDF.collect.foreach(println)
    assert(mergedDF.collect.size == 3)
  }

  "A joined DF" should "have size 3" in {
    var mergedDF = MergeUtils.joinOldAndNewDF(df1, df1.schema, null, df1.schema, "name")
    mergedDF.collect.foreach(println)
    assert(mergedDF.collect.size == 3)
  }

  "joinOldAndNew" should "return correct result" in {
    val keys = List(("name", "name"),("age","age"))
    val inner = MergeUtils.joinOldAndNew(oldDF,newDF,keys,SQL.INNER)
    val leftOuter = MergeUtils.joinOldAndNew(oldDF,newDF,keys,SQL.LEFT_OUTER)
    val fullOuter = MergeUtils.joinOldAndNew(oldDF,newDF,keys,SQL.FULL_OUTER)
    assert(inner.count() == 4)
    assert(leftOuter.count() == 6)
    assert(fullOuter.count() == 7)
    val expectedSchema = StructType(Array(
      StructField("age", LongType, true),
      StructField("name", StringType, true),
      StructField("new_age", LongType, true),
      StructField("new_name", StringType, true)))
    assert(inner.schema == leftOuter.schema && leftOuter.schema == fullOuter.schema && fullOuter.schema == expectedSchema)
  }
  //  override def afterAll() {
  //    super.afterAll()
  //  }

}