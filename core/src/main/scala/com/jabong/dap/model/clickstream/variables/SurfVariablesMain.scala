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
    val pagevisit: DataFrame = GetMergedClickstreamData.mergeAppsWeb(hiveContext, args(0))
    var UserObj = new GroupData(hiveContext, pagevisit)
    UserObj.calculateColumns()
    //val userWiseData: RDD[(String, Row)] = UserObj.groupDataByUser()
    //val browserWiseData: RDD[(String, Row)] = UserObj.groupDataByBrowser()
    //val surfVariableData: RDD[(Any, Row)] = UserObj.surfVariableData()
    //var surf3 = VariableMethods.Surf3(userWiseData, pagevisit, UserObj, hiveContext)
    //var variableSurfUserwise=VariableMethods.variableSurf1(surfVariableData,UserObj)
    //var variableSurfFinal=VariableMethods.variableSurf1(surfVariableData,UserObj)
    val finalUidDeviceid = VariableMethods.uidToDeviceid(hiveContext).write.save(args(1))
    //val finalSurf1Data=variableSurfUserwise.union(variableSurfBrowserwise)
    //variableSurfFinal.saveAsTextFile(args(1))
  }


  /*def relpaceNullUserid(GroupedData: RDD[(String, Row)]): RDD[((Any,Any,Any,Any),Any)] = {
    val a = GroupedData.filter((x => x._2(UserObj.productsku) != null))
    val b = a.map(x => ((x._2(userid), x._2(actualvisitid), x._2(browserid), x._2(domain)), x._2(productsku)))
    val c = b.reduceByKey((x, y) => (x + "," + y))
    return c
  }
  */
  def coalesce(id1: Any, id2: Any): String = {
    if (id1 == null)
      return id2.toString
    else
      return id1.toString
  }

  //def fullName: String = ClickstreamConstants.database + "." + ClickstreamConstants.tablename

}

