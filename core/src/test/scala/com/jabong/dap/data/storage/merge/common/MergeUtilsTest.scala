package com.jabong.dap.data.storage.merge.common

import com.jabong.dap.common.{ SharedSparkContext, Spark }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

class MergeUtilsTest extends FlatSpec with SharedSparkContext {

  @transient var df1: DataFrame = _
  @transient var df2: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    val sqlContext = Spark.getSqlContext()

    df1 = sqlContext.read.json(DataSets.TEST_RESOURCES + "common/merge/1.json")
    df2 = sqlContext.read.json(DataSets.TEST_RESOURCES + "common/merge/2.json")
    df1.collect.foreach(println)
  }

  "A Merged DF" should "have size 3" in {
    var mergedDF = MergeUtils.InsertUpdateMerge(df1, df2, "name")
    mergedDF.collect.foreach(println)
    assert(mergedDF.collect.size == 3)
  }

  //  override def afterAll() {
  //    super.afterAll()
  //  }

}
