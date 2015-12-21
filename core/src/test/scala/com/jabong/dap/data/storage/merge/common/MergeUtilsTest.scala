package com.jabong.dap.data.storage.merge.common

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.merge.MergeDelegator
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ StructField, StringType, LongType, StructType }
import org.scalatest.FlatSpec

class MergeUtilsTest extends FlatSpec with SharedSparkContext {

  @transient var df1: DataFrame = _
  @transient var df2: DataFrame = _

  @transient var oldDF: DataFrame = _
  @transient var newDF: DataFrame = _

  @transient var prevFullDf: DataFrame = _
  @transient var incrDf: DataFrame = _


  override def beforeAll() {
    super.beforeAll()

    df1 = JsonUtils.readFromJson("common/merge", "1")
    df2 = JsonUtils.readFromJson("common/merge", "2")
    oldDF = JsonUtils.readFromJson("common/merge", "mergeOld")
    newDF = JsonUtils.readFromJson("common/merge", "mergeNew")
    prevFullDf = JsonUtils.readFromJson("common/merge", "prevFull")
    incrDf = JsonUtils.readFromJson("common/merge", "incr")
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
  //
  //  "joinOldAndNew" should "return correct result" in {
  //    val expectedSchema = StructType(Array(
  //      StructField("age", LongType, true),
  //      StructField("name", StringType, true),
  //      StructField("new_age", LongType, true),
  //      StructField("new_name", StringType, true)))
  //    val oldSchema = StructType(Array(
  //      StructField("age", LongType, true),
  //      StructField("name", StringType, true)))
  //    val newSchema = StructType(Array(
  //      StructField("age", LongType, true),
  //      StructField("name", StringType, true)))
  //    val keys = List(("name", "name"), ("age", "age"))
  //    val inner = MergeUtils.joinOldAndNew(newDF, newSchema, oldDF, oldSchema, keys, SQL.INNER)
  //    val leftOuter = MergeUtils.joinOldAndNew(newDF, newSchema, oldDF, oldSchema, keys, SQL.LEFT_OUTER)
  //    val fullOuter = MergeUtils.joinOldAndNew(newDF, newSchema, oldDF, oldSchema, keys, SQL.FULL_OUTER)
  //    assert(inner.count() == 4)
  //    assert(leftOuter.count() == 6)
  //    assert(fullOuter.count() == 7)
  //
  //    assert(inner.schema == leftOuter.schema && leftOuter.schema == fullOuter.schema && fullOuter.schema == expectedSchema)
  //
  //    val mergedNewNull1 = MergeUtils.joinOldAndNew(newDF, newSchema, null, oldSchema, keys, SQL.FULL_OUTER)
  //    assert(mergedNewNull1.collect().size == 5)
  //
  //    val mergedNewNull2 = MergeUtils.joinOldAndNew(null, newSchema, oldDF, oldSchema, keys, SQL.FULL_OUTER)
  //    assert(mergedNewNull2.collect().size == 6)
  //  }
  "joinOldAndNew" should "return correct result" in {
    val expectedSchema = StructType(Array(
      StructField("age", LongType, true),
      StructField("name", StringType, true),
      StructField("new_age", LongType, true),
      StructField("new_name", StringType, true)))
    val oldSchema = StructType(Array(
      StructField("age", LongType, true),
      StructField("name", StringType, true)))
    val newSchema = StructType(Array(
      StructField("age", LongType, true),
      StructField("name", StringType, true)))
    val keys = List(("name", "name"), ("age", "age"))
    val inner = MergeUtils.joinOldAndNew(newDF, newSchema, oldDF, oldSchema, keys, SQL.INNER)
    val leftOuter = MergeUtils.joinOldAndNew(newDF, newSchema, oldDF, oldSchema, keys, SQL.LEFT_OUTER)
    val fullOuter = MergeUtils.joinOldAndNew(newDF, newSchema, oldDF, oldSchema, keys, SQL.FULL_OUTER)
    assert(inner.count() == 3)
    assert(leftOuter.count() == 4)
    assert(fullOuter.count() == 4)

    assert(inner.schema == leftOuter.schema && leftOuter.schema == fullOuter.schema && fullOuter.schema == expectedSchema)

    val mergedNewNull1 = MergeUtils.joinOldAndNew(newDF, newSchema, null, oldSchema, keys, SQL.FULL_OUTER)
    assert(mergedNewNull1.collect().size == 3)

    val mergedNewNull2 = MergeUtils.joinOldAndNew(null, newSchema, oldDF, oldSchema, keys, SQL.FULL_OUTER)
    assert(mergedNewNull2.collect().size == 4)

    val singleKey = MergeUtils.joinOldAndNew(newDF, newSchema, oldDF, oldSchema, List(("name", "name")), SQL.FULL_OUTER)
    assert(singleKey.count() == 6)
  }

  "insertUpdateMerge" should "return correct result" in {
    val keys = List("name", "age")
    val mergedDf = MergeUtils.InsertUpdateMerge(prevFullDf,incrDf,keys)
    mergedDf.show(10)
    assert(mergedDf.collect().size > 0)
    assert(mergedDf.collect().size == 5)
  }

}