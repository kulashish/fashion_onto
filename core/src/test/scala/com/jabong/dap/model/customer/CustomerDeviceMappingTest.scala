package com.jabong.dap.model.customer

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.model.customer.data.CustomerDeviceMapping
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by mubarak on 24/7/15.
 */
class CustomerDeviceMappingTest extends FlatSpec with SharedSparkContext {

  @transient var df: DataFrame = _
  @transient var df1: DataFrame = _
  @transient var df2: DataFrame = _
  @transient var df3: DataFrame = _
  @transient var df4: DataFrame = _

  override def beforeAll() {
    super.beforeAll()

    df = JsonUtils.readFromJson(DataSets.EXTRAS, DataSets.DEVICE_MAPPING, Schema.customerDeviceMapping)

    df1 = JsonUtils.readFromJson(DataSets.CUSTOMER, DataSets.CUSTOMER, Schema.customer)

    df2 = JsonUtils.readFromJson(DataSets.CLICKSTREAM, "userDeviceMappingOutput")

    df4 = JsonUtils.readFromJson(DataSets.EXTRAS, "device_mapping_1", Schema.customerDeviceMapping)

  }

  "Testing getLatestDevice method" should " return size 27" in {

    df3 = CustomerDeviceMapping.getDataFrameCsv4mDCF(JsonUtils.TEST_RESOURCES + "/" + DataSets.EXTRAS + "/device_mapping.csv")

    val res = CustomerDeviceMapping.getLatestDevice(df2, df3, df1)

    assert(res.collect().length == 27)
  }

  "Testing getLatestDevice method" should " match the output dataframe" in {

    val res = CustomerDeviceMapping.getLatestDevice(df2, df, df1)
    df.collect().foreach(println)
    df1.collect().foreach(println)
    df2.collect().foreach(println)
    res.collect().foreach(println)

    assert(res.collect().toSet.equals(df4.collect().toSet))
  }

}
