package com.jabong.dap.model.clickstream

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema._
import com.jabong.dap.model.clickstream.campaignData.AppDetails
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

import scala.collection.mutable.HashMap
import scala.reflect.io.File

/**
 * Created by kapil on 27/10/15.
 */
class AppDetailsTest extends FlatSpec with SharedSparkContext {

  def areValuesSame(d1: DataFrame, d2: DataFrame):Boolean = {
    d1.collect().toSet.equals(d2.collect().toSet)
  }

  def isSameDF(d1: DataFrame, d2:DataFrame): Boolean = {
    val isSameSchema = d1.schema.toSet === d2.schema.toSet
    if(!isSameSchema)
      return false
    val isCountSame = d1.distinct.count() == d2.distinct.count()
    if(!isCountSame)
      return false
    if(!areValuesSame(d1, d2))
      return false
    true
  }
  "transform" should " return correct hashMap " in {
    val master = JsonUtils.readFromJson(DataSets.CLICKSTREAM + File.separator + "appDetails", "master", masterSchema)
    System.out.println(master.show())
    val salesOrderDF = JsonUtils.readFromJson(DataSets.CLICKSTREAM + File.separator + "appDetails", "salesOrder", salesOrder)
    val cmr = JsonUtils.readFromJson(DataSets.CLICKSTREAM + File.separator + "appDetails", "cmr", cmrSchema)
    val customerSession = JsonUtils.readFromJson(DataSets.CLICKSTREAM + File.separator + "appDetails", "customerSession", customerSessionSchema)

    val expectedMaster = JsonUtils.readFromJson(DataSets.CLICKSTREAM + File.separator + "appDetails", "expectedMaster", masterSchema)
    val expectedIncr = JsonUtils.readFromJson(DataSets.CLICKSTREAM + File.separator + "appDetails", "expectedIncr", masterSchema)


    var dfMap: HashMap[String, DataFrame] = new HashMap[String, DataFrame]()
    dfMap.put("masterRecord", master)
    dfMap.put("salesOrder", salesOrderDF)
    dfMap.put("cmr", cmr)
    dfMap.put("customerSession",customerSession)

    val resultMap = AppDetails.process(dfMap)
//    resMaster.limit(30).write.json(DataSets.CLICKSTREAM + File.separator + "appDetails/" + "expMaster" + ".json")
//    resIncr.limit(30).write.json(DataSets.CLICKSTREAM + File.separator + "appDetails/" + "expIncr.json")
    val resMaster = resultMap("updatedMaster")
    val resIncr = resultMap("incrDF")
    assert(isSameDF(resMaster, expectedMaster) && isSameDF(resIncr, expectedIncr))
  }
}
