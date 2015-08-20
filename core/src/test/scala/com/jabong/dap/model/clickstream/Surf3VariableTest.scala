package com.jabong.dap.model.clickstream

import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{SharedSparkContext, Spark}
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.clickstream.utils.{GroupData, UserAttribution}
import com.jabong.dap.model.clickstream.variables.GetSurfVariables
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.scalatest.FlatSpec

/**
 * Created by Divya on 14/7/15.
 */
class Surf3VariableTest extends FlatSpec with SharedSparkContext {
  var sqlContext: SQLContext = _
  @transient var pagevisitDataFrame: DataFrame = _
  @transient var userObj: GroupData = _
  @transient var userWiseData: RDD[(String, Row)] = _
  @transient var hiveContext: HiveContext = _
  @transient var YesterMergedData: DataFrame = _
  @transient var dailyIncrementalSKUs: DataFrame = _
  @transient var pagevisitDataFrame2: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    hiveContext = Spark.getHiveContext()
    sqlContext = Spark.getSqlContext()

    pagevisitDataFrame = JsonUtils.readFromJson(DataSets.CLICKSTREAM, "pagevisitSingleUser")
    pagevisitDataFrame2 = JsonUtils.readFromJson(DataSets.CLICKSTREAM, "pagevisitMultiUser")
    YesterMergedData = JsonUtils.readFromJson(DataSets.CLICKSTREAM, "Surf3MergedData")
  }

  "DailyIncremetal for surf3" should "assert to given no of unique PDP records for given user" in {
    var today = "_daily"
    var attributeObj: UserAttribution = new UserAttribution(hiveContext, sqlContext, pagevisitDataFrame)
    var userAttributedData: DataFrame = attributeObj.attribute()

    userObj = new GroupData()
    var useridDeviceidFrame = userObj.appuseridCreation(userAttributedData)

    userObj.calculateColumns(useridDeviceidFrame)
    userWiseData = userObj.groupDataByAppUser(hiveContext, useridDeviceidFrame)

    dailyIncrementalSKUs = GetSurfVariables.Surf3Incremental(userWiseData, userObj, hiveContext)
    assert(dailyIncrementalSKUs.filter("userid_daily = 'user1'").count() == 10)
    assert(dailyIncrementalSKUs.filter("userid_daily = '_app_b1'").count() == 2)
  }

  /* "AppCreation for surf3" should "have 12 unique PDP records " in {
    var today = "_daily"
    userObj = new GroupData()
    var AppDataFrame = userObj.appuseridCreation
    userObj.calculateColumns(AppDataFrame)
    userWiseData = userObj.groupDataByAppUser(AppDataFrame)

    dailyIncrementalSKUs = GetSurfVariables.Surf3Incremental(userWiseData, userObj, hiveContext)
    assert(dailyIncrementalSKUs.count() == 12)
  }
*/
  "merged data " should "have onlu 2 skus matching in 2-30 days" in {
    var surf3Variable = GetSurfVariables.ProcessSurf3Variable(YesterMergedData, dailyIncrementalSKUs)
    assert(surf3Variable.count() == 2)
  }

  /**
   * Today's merge should only have the data for last 29 days and should filyet out the data before that
   */
  "Today's merge" should "have 6 rows" in {
    var mergeForTom = GetSurfVariables.mergeSurf3Variable(hiveContext, YesterMergedData, dailyIncrementalSKUs, "2015/07/18")
    assert(mergeForTom.count() == 6)
  }

}
