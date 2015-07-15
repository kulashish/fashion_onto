package com.jabong.dap.model.clickstream.variables

import com.jabong.dap.common.Spark
import com.jabong.dap.model.clickstream.utils.{GroupData, GetMergedClickstreamData}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SaveMode, Row}
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.immutable.HashMap

/**
 * Created by Divya on 13/7/15.
 */
object SurfVariablesMain extends java.io.Serializable {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Clickstream Surf Variables").set("spark.driver.allowMultipleContexts", "true")
    Spark.init(conf)
    val hiveContext = Spark.getHiveContext()
    val tablename = args(0)
    val pagevisit: DataFrame = GetMergedClickstreamData.mergeAppsWeb(hiveContext, tablename)
    var UserObj = new GroupData(hiveContext, pagevisit)
    UserObj.calculateColumns()
    val userWiseData: RDD[(String, Row)] = UserObj.groupDataByUser()
    val browserWiseData: RDD[(String, Row)] = UserObj.groupDataByBrowser()
    var surf3IncrementVariables = GetSurfVariables.Surf3(userWiseData, UserObj, hiveContext)
    surf3IncrementVariables.take(8) foreach (println)
  }
  def coalesce(id1: Any, id2: Any): String = {
    if (id1 == null)
      return id2.toString
    else
      return id1.toString
  }
  //def fullName: String = ClickstreamConstants.database + "." + ClickstreamConstants.tablename

}

