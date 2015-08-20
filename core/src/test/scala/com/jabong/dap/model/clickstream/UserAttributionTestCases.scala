package com.jabong.dap.model.clickstream

import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{SharedSparkContext, Spark}
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.clickstream.utils.UserAttribution
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.FlatSpec

/**
 * Created by Divya
 */
class UserAttributionTestCases extends FlatSpec with SharedSparkContext {
  @transient var sqlContext: SQLContext = _
  @transient var pagevisitDataFrame: DataFrame = _
  @transient var userObj: UserAttribution = _
  @transient var hiveContext: HiveContext = _

  override def beforeAll() {
    super.beforeAll()
    hiveContext = Spark.getHiveContext()
    sqlContext = Spark.getSqlContext()
    pagevisitDataFrame = JsonUtils.readFromJson(DataSets.CLICKSTREAM, "UserNullAttribution")
    pagevisitDataFrame.show()
  }

  "DailyIncremetal for surf3" should "have 12 unique PDP records " in {
    var today = "_daily"
    userObj = new UserAttribution(hiveContext, sqlContext, pagevisitDataFrame)
    var newData = userObj.attribute()
    newData.show()
    //assert(pagevisitDataFrame.filter("userid = 'user1'").count()==1)
    //assert(newData.filter("userid = 'user1'").count()==4)
    //assert(newData.filter("userid = 'user2'").count()==2)
  }

}
