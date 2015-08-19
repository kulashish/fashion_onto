package com.jabong.dap.model.clickstream

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.clickstream.variables.UserDeviceMapping
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

class UserDeviceMappingTest extends FlatSpec with SharedSparkContext {

  @transient var dfInputUserDeviceMap: DataFrame = _
  @transient var dfOutputUserDeviceMap: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    dfInputUserDeviceMap = JsonUtils.readFromJson(DataSets.CLICKSTREAM, "userDeviceMappingInput")
    dfOutputUserDeviceMap = JsonUtils.readFromJson(DataSets.CLICKSTREAM, "userDeviceMappingOutput")
  }

 "getUserDeviceMapApp: (null)" should " be null " in {
    var udMap = UserDeviceMapping.getUserDeviceMapApp(null)
    assert(udMap == null)
  }

  "getUserDeviceMapApp: (DF)" should "have 10 records only " in {
    var udMap = UserDeviceMapping.getUserDeviceMapApp(dfInputUserDeviceMap)
    assert(udMap.collect.size == 10)
  }

  "getUserDeviceMapApp: (DF)" should " match the output DF" in {
    var udMap = UserDeviceMapping.getUserDeviceMapApp(dfInputUserDeviceMap).collect().toSet()
    assert(udMap.equals(dfOutputUserDeviceMap.collect().toSet()) == true)
  }

}
