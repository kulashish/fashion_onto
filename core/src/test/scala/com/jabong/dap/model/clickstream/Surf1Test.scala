package com.jabong.dap.model.clickstream

/**
 * Created by udit on 14/7/15.
 */

import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{ SharedSparkContext, Spark }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.clickstream.utils.GroupData
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.scalatest.FlatSpec

/**
 *  basic recommender test cases
 */
class Surf1Test extends FlatSpec with SharedSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var hiveContext: HiveContext = _
  @transient var pagevisitDataFrame: DataFrame = _
  @transient var userObj: GroupData = _
  @transient var surfVariableData: RDD[(String, Row)] = _
  //@transient var orderItemDataFrame: DataFrame = _
  //var clickstream: VariableMethods = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()

    //basicRecommender = new BasicRecommender()
    // orderItemDataFrame = JsonUtils.readFromJson(DataSets.SALES_CART, "OrderItemHistory")
    pagevisitDataFrame = JsonUtils.readFromJson(DataSets.CLICKSTREAM, "SurfVariables/surf1")
    //    println("After Json Read")
    pagevisitDataFrame.foreach(println)
    userObj = new GroupData()
    userObj.calculateColumns(pagevisitDataFrame)
    //surfVariableData = userObj.groupDataByAppUser(pagevisitDataFrame)

    //surfVariableData.collect().foreach(println)
    //testDataFrame = JsonUtils.readFromJson("sales_cart", "SalesCartEmpty")
  }
  /*
  "Given userid" should "return number of sessions visited" in {
    val result1 = GetSurfVariables.variableSurf1(pagevisitDataFrame,userObj,hiveContext)
    val result2 =result1.filter("appuid ='YncVoQTjiRGHuyoJTuD7FMF7+e2qAxm3tGhdx0LfVzk='")
    assert(result2.count()== 1)
  }
*/
  /*
  "Given userid" should "return number of sessions visited" in {
    val result1 = GetSurfVariables.variableSurf1(pagevisitDataFrame,userObj,hiveContext)
    val result2 =result1.map(x=>x._1).filter(x=>x.contains("YncVoQTjiRGHuyoJTuD7FMF7+e2qAxm3tGhdx0LfVzk="))
    assert(result2.count()== 1)
  }



  "null userid for desktop" should "remain unchanged" in {
    val result1 = GetSurfVariables.variableSurf1(pagevisitDataFrame,userObj,hiveContext)
    val result2 =result1.map(x=>x._1).filter(x=>x.contains("null") && (x.contains("w")||x.contains("m")))
    assert(result2.count()== 0)
  }
  "null userid for app" should "convert to _app_browserid format" in {
    val result1 = GetSurfVariables.variableSurf1(pagevisitDataFrame,userObj,hiveContext)
    val result2 =result1.map(x=>x._1).filter(x=>x.contains("_app_") && (x.contains("android")||x.contains("ios")|| x.contains("windows")))
    assert(result2.count()== 0)
  }
  "Given userid and session id combination " should "return one record for sku browsed" in {
    val result1 = GetSurfVariables.variableSurf1(pagevisitDataFrame,userObj,hiveContext)
    val result2 =result1.map(x=>x._1).filter(x=>x.contains("YncVoQTjiRGHuyoJTuD7FMF7+e2qAxm3tGhdx0LfVzk=") && x.contains("559ede1324c17f349b8b456c-1"))
    assert(result2.count()== 1)
  }
  */
}