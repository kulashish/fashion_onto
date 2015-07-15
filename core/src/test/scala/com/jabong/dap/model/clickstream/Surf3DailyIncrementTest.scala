package com.jabong.dap.model.clickstream

import com.jabong.dap.common.{Spark, SharedSparkContext}
import com.jabong.dap.model.clickstream.variables.{GetSurfVariables, VariableMethods}
import org.apache.spark.sql.{DataFrame, SQLContext,Row}
import org.scalatest.FlatSpec
import com.jabong.dap.model.clickstream.utils.GroupData
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by Divya on 14/7/15.
 */
class Surf3DailyIncrementTest extends FlatSpec with SharedSparkContext {
  @transient var sqlContext: SQLContext = _
  @transient var pagevisitDataFrame: DataFrame = _
  @transient var userObj: GroupData = _
  @transient var userWiseData: RDD[(String, Row)] = _
  @transient var hiveContext: HiveContext = _

  override def beforeAll() {
    super.beforeAll()
    val hiveContext = Spark.getHiveContext()
    sqlContext = Spark.getSqlContext()
    pagevisitDataFrame = sqlContext.read.json("src/test/resources/clickstream/pagevisitSingleUser.json")
    userObj = new GroupData(hiveContext, pagevisitDataFrame)
    userWiseData = userObj.groupDataByUser()
    userObj.calculateColumns()
  }

  "DailyIncremetal for surf3" should "have 27 PDP records " in {
    var dailyIncrementalSKUs = GetSurfVariables.Surf3(userWiseData, userObj, hiveContext)
    dailyIncrementalSKUs.collect.foreach(println)
    assert(dailyIncrementalSKUs.collect.size == 27)
  }

}
