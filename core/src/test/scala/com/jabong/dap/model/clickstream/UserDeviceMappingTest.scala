package com.jabong.dap.model.clickstream

import com.jabong.dap.common.{ SharedSparkContext, Spark }
import com.jabong.dap.model.clickstream.variables.{ UserDeviceMapping }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.scalatest.FlatSpec

class UserDeviceMappingTest extends FlatSpec with SharedSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var dfInputUserDeviceMap: DataFrame = _
  @transient var dfOutputUserDeviceMap: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    dfInputUserDeviceMap = sqlContext.read.json(DataSets.TEST_RESOURCES + DataSets.CLICKSTREAM + "/userDeviceMappingInput.json")
    dfOutputUserDeviceMap = sqlContext.read.json(DataSets.TEST_RESOURCES + DataSets.CLICKSTREAM + "/userDeviceMappingOutput.json")
  }

  "getUserDeviceMap: (null)" should " be null " in {
    var udMap = UserDeviceMapping.getUserDeviceMap(null)
    assert(udMap == null)
  }

  "getUserDeviceMap: (DF)" should "have 16 records only " in {
    var udMap = UserDeviceMapping.getUserDeviceMap(dfInputUserDeviceMap)
    assert(udMap.collect.size == 16)
  }

  "getUserDeviceMap: (DF)" should " match the output DF" in {
    var udMap = UserDeviceMapping.getUserDeviceMap(dfInputUserDeviceMap).collect().toSet()
    assert(udMap.equals(dfOutputUserDeviceMap.collect().toSet()) == true)
  }

  "getUserDeviceMapApp: (null)" should " be null " in {
    var udMap = UserDeviceMapping.getUserDeviceMapApp(null)
    assert(udMap == null)
  }

  "getUserDeviceMapApp: (DF)" should "have 16 records only " in {
    var udMap = UserDeviceMapping.getUserDeviceMapApp(dfInputUserDeviceMap)
    assert(udMap.collect.size == 16)
  }

  "getUserDeviceMapApp: (DF)" should " match the output DF" in {
    var udMap = UserDeviceMapping.getUserDeviceMapApp(dfInputUserDeviceMap).collect().toSet()
    assert(udMap.equals(dfOutputUserDeviceMap.collect().toSet()) == true)
  }

}