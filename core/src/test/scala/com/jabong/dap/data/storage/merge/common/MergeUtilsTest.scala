package com.jabong.dap.data.storage.merge.common

import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{SharedSparkContext, Spark}
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

class MergeUtilsTest extends FlatSpec with SharedSparkContext {

  @transient var df1: DataFrame = _
  @transient var df2: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    val sqlContext = Spark.getSqlContext()

    df1 = JsonUtils.readFromJson("common/merge", "1")
    df2 = JsonUtils.readFromJson("common/merge", "2")
    df1.collect.foreach(println)
  }

  "A Merged DF" should "have size 4" in {
    var mergedDF = MergeUtils.InsertUpdateMerge(df1, df2, "name")
    mergedDF.collect.foreach(println)
    assert(mergedDF.collect.size == 4)
  }

  "A merged DF" should "have size 3" in {
    var mergedDF = MergeUtils.joinOldAndNewDF(df1, df1.schema, null, df1.schema, "name")
    mergedDF.collect.foreach(println)
    assert(mergedDF.collect.size == 3)
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

  //  override def afterAll() {
  //    super.afterAll()
  //  }

}