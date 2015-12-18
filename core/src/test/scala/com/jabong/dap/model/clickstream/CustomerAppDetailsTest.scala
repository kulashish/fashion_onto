package com.jabong.dap.model.clickstream

import com.jabong.dap.common.{ TestSchema, SharedSparkContext }
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.model.clickstream.campaignData.CustomerAppDetails
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

import scala.collection.mutable.HashMap
import scala.reflect.io.File

/**
 * Created by kapil on 27/10/15.
 */
class CustomerAppDetailsTest extends FlatSpec with SharedSparkContext {

  def areValuesSame(d1: DataFrame, d2: DataFrame): Boolean = {
    d1.collect().toSet.equals(d2.collect().toSet)
  }

  def isSameDF(d1: DataFrame, d2: DataFrame): Boolean = {
    val isSameSchema = d1.schema.toSet === d2.schema.toSet
    if (!isSameSchema)
      return false
    val isCountSame = d1.distinct.count() == d2.distinct.count()
    if (!isCountSame)
      return false
    if (!areValuesSame(d1, d2))
      return false
    true
  }
  "process" should " return correct hashMap " in {
    val master = JsonUtils.readFromJson(DataSets.CLICKSTREAM + File.separator + "appDetails", "master", TestSchema.customerAppDetails)
    System.out.println(master.show())
    val salesOrderDF = JsonUtils.readFromJson(DataSets.CLICKSTREAM + File.separator + "appDetails", "salesOrder", Schema.salesOrder)
    val cmr = JsonUtils.readFromJson(DataSets.CLICKSTREAM + File.separator + "appDetails", "cmr", Schema.cmr)
    val customerSession = JsonUtils.readFromJson(DataSets.CLICKSTREAM + File.separator + "appDetails", "customerSession", TestSchema.customerSession)

    val expectedMaster = JsonUtils.readFromJson(DataSets.CLICKSTREAM + File.separator + "appDetails", "expectedMaster", TestSchema.customerAppDetails)
    val expectedIncr = JsonUtils.readFromJson(DataSets.CLICKSTREAM + File.separator + "appDetails", "expectedIncr", TestSchema.customerAppDetails)

    var dfMap: HashMap[String, DataFrame] = new HashMap[String, DataFrame]()
    dfMap.put("custAppDetailsPrevFull", master)
    dfMap.put("salesOrderIncr", salesOrderDF)
    dfMap.put("cmrFull", cmr)
    dfMap.put("customerSessionIncr", customerSession)

    val resultMap = CustomerAppDetails.process(dfMap)
    //    resMaster.limit(30).write.json(DataSets.CLICKSTREAM + File.separator + "appDetails/" + "expMaster" + ".json")
    //    resIncr.limit(30).write.json(DataSets.CLICKSTREAM + File.separator + "appDetails/" + "expIncr.json")
    val resMaster = resultMap("custAppDetailsFull")
    val resIncr = resultMap("custAppDetailsIncr")
    assert(resMaster.count() == 11 && resIncr.count() == 4)
    assert(resMaster.count() == resMaster.select("id_customer", "domain").distinct.count())
  }
}
