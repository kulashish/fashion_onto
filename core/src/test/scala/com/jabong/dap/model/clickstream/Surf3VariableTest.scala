package com.jabong.dap.model.clickstream

import java.util.Calendar

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
class Surf3VariableTest extends FlatSpec with SharedSparkContext {
  @transient var sqlContext: SQLContext = _
  @transient var pagevisitDataFrame: DataFrame = _
  @transient var userObj: GroupData = _
  @transient var userWiseData: RDD[(String, Row)] = _
  @transient var hiveContext: HiveContext = _
  @transient var YesterMergedData: DataFrame = _
  @transient var dailyIncrementalSKUs: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    hiveContext = Spark.getHiveContext()
    sqlContext = Spark.getSqlContext()
    pagevisitDataFrame = sqlContext.read.json("src/test/resources/clickstream/pagevisitSingleUser.json")
    YesterMergedData = sqlContext.read.json("src/test/resources/clickstream/Surf3MergedData")
  }

  "DailyIncremetal for surf3" should "have 12 unique PDP records " in {
    var today = "_daily"
    userObj = new GroupData(hiveContext, pagevisitDataFrame)
    userObj.calculateColumns(pagevisitDataFrame)
    userWiseData = userObj.groupDataByAppUser(pagevisitDataFrame)

    dailyIncrementalSKUs = GetSurfVariables.Surf3Incremental(userWiseData, userObj, hiveContext)
    assert(dailyIncrementalSKUs.count() == 12)
  }


  "merged data " should "have onlu 2 skus matching in 2-30 days" in {
    var surf3Variable = GetSurfVariables.ProcessSurf3Variable(YesterMergedData,dailyIncrementalSKUs)
    assert(surf3Variable.count() == 2)
  }

  /**
   * Today's merge should only have the data for last 29 days and should filyet out the data before that
    */
  "Today's merge" should "have 5 rows" in {
    var mergeForTom = GetSurfVariables.mergeSurf3Variable(hiveContext,YesterMergedData,dailyIncrementalSKUs,"18/07/2015")
    assert(mergeForTom.count() == 5)
  }

}
