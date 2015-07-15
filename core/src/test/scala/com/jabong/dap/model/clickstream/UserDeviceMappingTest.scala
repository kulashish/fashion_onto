package com.jabong.dap.model.clickstream

import com.jabong.dap.common.{SharedSparkContext, Spark}
import com.jabong.dap.model.clickstream.utils.GroupData
import com.jabong.dap.model.clickstream.variables.{VariableMethods, UserDeviceMapping}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.scalatest.FlatSpec

class UserDeviceMappingTest extends FlatSpec with SharedSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var pagevisitDataFrame: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    pagevisitDataFrame = sqlContext.read.json("src/test/resources/clickstream/pagevisitSingleUser.json")
  }

  "getAppUserDeviceMap: for app users " should "have some records " in {
    var dailyIncrementalSKUs = UserDeviceMapping.getAppUserDeviceMap(pagevisitDataFrame)

    dailyIncrementalSKUs.collect.foreach(println)

    assert(dailyIncrementalSKUs.collect.size != 0)
  }
}
