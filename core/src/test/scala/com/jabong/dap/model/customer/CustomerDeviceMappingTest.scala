package com.jabong.dap.model.customer

import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{ SharedSparkContext, TestSchema }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.customer.data.CustomerDeviceMapping
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by mubarak on 24/7/15.
 */
class CustomerDeviceMappingTest extends FlatSpec with SharedSparkContext {

  @transient var cus1: DataFrame = _
  @transient var cus2: DataFrame = _
  @transient var click1: DataFrame = _
  @transient var click2: DataFrame = _
  @transient var dcf: DataFrame = _
  @transient var res1: DataFrame = _
  @transient var res2: DataFrame = _

  override def beforeAll() {
    super.beforeAll()

    cus1 = JsonUtils.readFromJson(DataSets.EXTRAS, "customer1")

    cus2 = JsonUtils.readFromJson(DataSets.EXTRAS, "customer2")

    click1 = JsonUtils.readFromJson(DataSets.EXTRAS, "clickstream")

    click2 = JsonUtils.readFromJson(DataSets.EXTRAS, "clickstream1")

    dcf = JsonUtils.readFromJson(DataSets.EXTRAS, "device_mapping", TestSchema.customerDeviceMapping)

    res1 = JsonUtils.readFromJson(DataSets.EXTRAS, "res1", TestSchema.customerDeviceMapping)

    res2 = JsonUtils.readFromJson(DataSets.EXTRAS, "res2", TestSchema.customerDeviceMapping)

  }

  "Testing getLatestDevice method 1" should "match the output dataframe" in {

    val res = CustomerDeviceMapping.getLatestDevice(click1, dcf, cus1, null)
    assert(res.collect().toSet.equals(res1.collect().toSet))
  }

  "Testing getLatestDevice method" should " match the output dataframe" in {

    val res = CustomerDeviceMapping.getLatestDevice(click2, dcf, cus2, null)
    assert(res.collect().toSet.equals(res2.collect().toSet))
  }
}
