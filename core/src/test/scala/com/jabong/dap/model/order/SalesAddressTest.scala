package com.jabong.dap.model.order

import com.jabong.dap.common.{Spark, SharedSparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.FlatSpec
import com.jabong.dap.model.order.variables.SalesOrderAddress


/**
 * Created by jabong on 2/7/15.
 */
class SalesAddressTest extends FlatSpec with SharedSparkContext{

@transient var sqlContext: SQLContext = _
@transient var df1: DataFrame = _
@transient var df2: DataFrame = _

override def beforeAll() {
super.beforeAll()
sqlContext = new SQLContext(Spark.getContext())

df1 = sqlContext.read.json("test/sales_address.json")
df2 = sqlContext.read.json("test/sales_address2.json")

df1.collect.foreach(println)
}


"Testing max mobile No and city" should "have size 3" in {
  var maxCityPhon = SalesOrderAddress.getFav(df1)
  maxCityPhon.collect().foreach(println)
  assert(maxCityPhon.collect.size == 3)
}
}
