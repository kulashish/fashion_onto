package com.jabong.dap.model.customer

import java.io.File

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

    dcf = JsonUtils.readFromJson(DataSets.EXTRAS, "device_mapping", Schema.customerDeviceMapping)

  }

  "Testing getLatestDevice method" should " return size 27" in {

 //   dcf = CustomerDeviceMapping.getDataFrameCsv4mDCF(JsonUtils.TEST_RESOURCES + File.separator + DataSets.EXTRAS + File.separator + "device_mapping.csv")

    val res = CustomerDeviceMapping.getLatestDevice(click1, dcf, cus1)

    click1.collect().foreach(println)
    dcf.collect().foreach(println)
    cus1.collect().foreach(println)
    res.collect().foreach(println)
    assert(res.collect().toSet.equals(res.collect().toSet))
  }

  "Testing getLatestDevice method" should " match the output dataframe" in {

    val res = CustomerDeviceMapping.getLatestDevice(click2, dcf, cus2)
    click2.collect().foreach(println)
    dcf.collect().foreach(println)
    cus2.collect().foreach(println)
    res.collect().foreach(println)

    assert(res.collect().toSet.equals(res.collect().toSet))
  }

}
