/*package com.jabong.dap.model.clickstream

/**
 * Created by udit on 14/7/15.
 */

import com.jabong.dap.model.clickstream.utils.GroupData
import com.jabong.dap.model.clickstream.variables.VariableMethods
import com.jabong.dap.common.{ SharedSparkContext, Spark }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.scalatest.FlatSpec


import scala.collection.mutable

/**
 *  basic recommender test cases
 */
class Surf1Test extends FlatSpec with SharedSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var hiveContext: HiveContext = _
  @transient var surfDataFrame: DataFrame = _
  @transient var userObj: GroupData = _
  @transient var surfVariableData: RDD[(Any, Row)] = _
  //@transient var orderItemDataFrame: DataFrame = _
  //var clickstream: VariableMethods = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()

    //basicRecommender = new BasicRecommender()
    // orderItemDataFrame = sqlContext.read.json("src/test/resources/salescart/OrderItemHistory.json")
    surfDataFrame = sqlContext.read.json("core/src/test/resources/clickstream/surfvariables/surf1-2-6.json")
//    println("After Json Read")
    surfDataFrame.foreach(println)
    userObj = new GroupData(hiveContext, surfDataFrame)
    userObj.calculateColumns()
    surfVariableData = userObj.surfVariableData()

    //surfVariableData.collect().foreach(println)
    //testDataFrame = sqlContext.read.json("src/test/resources/SalesCartEmpty.json")
  }

  "Given userid" should "return number of sessions visited" in {
    val result1 = VariableMethods.variableSurf1(surfVariableData, userObj)
    val result2 =result1.map(x=>x._1).filter(x=>x.contains("YncVoQTjiRGHuyoJTuD7FMF7+e2qAxm3tGhdx0LfVzk="))
    assert(result2.count()== 2)
  }

  "null userid for desktop" should "remain unchanged" in {
    val result1 = VariableMethods.variableSurf1(surfVariableData, userObj)
    val result2 =result1.map(x=>x._1).filter(x=>x.contains("null") && (x.contains("w")||x.contains("m")))
    assert(result2.count()== 2)
  }
  "null userid for app" should "convert to _app_browserid format" in {
    val result1 = VariableMethods.variableSurf1(surfVariableData, userObj)
    val result2 =result1.map(x=>x._1).filter(x=>x.contains("_app_") && (x.contains("android")||x.contains("ios")|| x.contains("windows")))
    assert(result2.count()== 3)
  }
  "Given userid and session id combination " should "return one record for sku browsed" in {
    val result1 = VariableMethods.variableSurf1(surfVariableData, userObj)
    val result2 =result1.map(x=>x._1).filter(x=>x.contains("YncVoQTjiRGHuyoJTuD7FMF7+e2qAxm3tGhdx0LfVzk=") && x.contains("559ede1324c17f349b8b456c-1"))
    assert(result2.count()== 1)
  }
}
*/