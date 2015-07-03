package com.jabong.dap.data.storage.merge.common

import com.jabong.dap.common.{SharedSparkContext, Spark}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.FlatSpec

class MergeUtilsTest extends FlatSpec with SharedSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var df1: DataFrame = _
  @transient var df2: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = new SQLContext(Spark.getContext())

    // TOFIX: make the path relative to test/resources
    df1 = sqlContext.read.json("test/1.json")
    df2 = sqlContext.read.json("test/2.json")
    df1.collect.foreach(println)
  }

  "A Merged DF" should "have size 3" in {
    var mergedDF = MergeUtils.InsertUpdateMerge(df1, df2, "name")
    mergedDF.collect.foreach(println)
    assert(mergedDF.collect.size == 3)
  }

}
