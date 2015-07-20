package com.jabong.dap.model.clickstream

import com.jabong.dap.common.{SharedSparkContext, Spark}
import com.jabong.dap.model.clickstream.variables.{UserDeviceMapping}
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.scalatest.FlatSpec

class UserDeviceMappingTest extends FlatSpec with SharedSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var userDeviceDf: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    userDeviceDf = sqlContext.read.json(DataSets.TEST_RESOURCES + DataSets.CLICKSTREAM + "/userDeviceMapping.json")
  }

  "getUserDeviceMap: (null)" should " be null " in {
    var udMap = UserDeviceMapping.getUserDeviceMap(null)
    assert(udMap == null)
  }

  "getUserDeviceMap: (DF)" should "have 16 records only " in {
    var udMap = UserDeviceMapping.getUserDeviceMap(userDeviceDf)
    assert(udMap.collect.size == 16)
  }

}