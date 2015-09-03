package com.jabong.dap.model.ad4push.data

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.schema.SchemaUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.ad4push.schema.DevicesReactionsSchema
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by jabong on 25/8/15.
 */
class Ad4pushDeviceMergerTest extends FlatSpec with SharedSparkContext {

  @transient var df1: DataFrame = _
  @transient var df2: DataFrame = _
  @transient var df3: DataFrame = _
  @transient var df4: DataFrame = _
  @transient var df5: DataFrame = _
  @transient var df6: DataFrame = _
  override def beforeAll() {
    super.beforeAll()
    df1 = JsonUtils.readFromJson(DataSets.AD4PUSH, "exportDevices/exportDevices_517_1")
    df2 = JsonUtils.readFromJson(DataSets.AD4PUSH, "exportDevices/exportDevices_517_2")
    df3 = JsonUtils.readFromJson(DataSets.AD4PUSH, "exportDevices/exportDevices_517_merged")

    df4 = JsonUtils.readFromJson(DataSets.AD4PUSH, "exportDevices/exportDevices_515_1")
    df5 = JsonUtils.readFromJson(DataSets.AD4PUSH, "exportDevices/exportDevices_515_2")
    df6 = JsonUtils.readFromJson(DataSets.AD4PUSH, "exportDevices/exportDevices_515_merged")
  }

  "Testing merging 517" should "have 14 recs" in {
    val t0 = System.nanoTime()
    var res = Ad4pushDeviceMerger.mergeExportData(SchemaUtils.changeSchema(df1, DevicesReactionsSchema.Ad4pushDeviceIOS), SchemaUtils.changeSchema(df2, DevicesReactionsSchema.Ad4pushDeviceIOS))
    val t1 = System.nanoTime()
    println("Normal time: " + (t1 - t0) + "ns")
    //assert(res.collect().toSet.equals(df3.collect().toSet))
    assert(res.collect().length == 14)
  }

  "Testing merging 515" should "have 17 recs" in {
    val t0 = System.nanoTime()
    var res = Ad4pushDeviceMerger.mergeExportData(df4, df5)
    val t1 = System.nanoTime()
    println("Normal time: " + (t1 - t0) + "ns")
    //assert(res.collect().toSet.equals(df6.collect().toSet))
    assert(res.collect().length == 17)
  }

}
