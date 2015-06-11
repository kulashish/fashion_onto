package com.jabong.dap.common

import org.apache.spark._
import org.scalatest.{BeforeAndAfter, FlatSpec}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame

class MergeUtilsTest extends FlatSpec with SharedSparkContext {

  @transient var sqlContext: SQLContext = _;
  @transient var df1 : DataFrame = _;
  @transient var df2: DataFrame = _;

  override def beforeAll()   {
    super.beforeAll()
    sqlContext = new SQLContext(sc)
    
    // TOFIX: make the path relative to test/resources
    df1 = sqlContext.jsonFile("test/1.json")
    df2 = sqlContext.jsonFile("test/2.json")
    df1.collect.foreach(println)
  }
  
  "A Merged DF" should "have size 3" in {
    var abc = MergeUtils.insertOrUpdateMerge(df1, df2, "name")
    abc.collect.foreach(println)
    assert(abc.collect.size == 3)
  }

}
