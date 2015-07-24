package com.jabong.dap.model.customer

import com.jabong.dap.common.{ SharedSparkContext}
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.model.customer.data.CustomerDeviceMapping
import org.apache.spark.sql.{ DataFrame}
import org.scalatest.FlatSpec

/**
 * Created by mubarak on 24/7/15.
 */
class CustomerDeviceMappingTest extends FlatSpec with SharedSparkContext {

  @transient var df1: DataFrame = _
  @transient var df2: DataFrame = _
  @transient var df3: DataFrame = _

  override def beforeAll() {
    super.beforeAll()

    df1 = JsonUtils.readFromJson(DataSets.CUSTOMER, DataSets.CUSTOMER, Schema.customer)

    df1.collect.foreach(println)

    df2 = JsonUtils.readFromJson(DataSets.CLICKSTREAM, "userDeviceMappingOutput")

    df2.printSchema()

    df2.show(5)


  }

  "Testing getLatestDevice method" should " return size 27" in {

    df3 = CustomerDeviceMapping.getDataFrameCsv4mDCF(JsonUtils.TEST_RESOURCES + "/" + DataSets.EXTRAS + "/device_mapping.csv")

    df3.show(5)

    val res = CustomerDeviceMapping.getLatestDevice(df2, df3, df1)

    res.printSchema()

    res.show(5)

    assert(res.collect().length == 27)
  }

}
