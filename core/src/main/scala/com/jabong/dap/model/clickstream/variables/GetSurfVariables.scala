package com.jabong.dap.model.clickstream.variables

import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.model.clickstream.utils.GroupData
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by Divya on 15/7/15.
 */
object GetSurfVariables extends java.io.Serializable {

  def Surf3Incremental(GroupedData: RDD[(String, Row)], UserObj: GroupData, hiveContext: HiveContext): DataFrame = {
    import hiveContext.implicits._
    val dailyIncremental = GroupedData.filter(v => v._2(UserObj.pagetype) == "CPD" || v._2(UserObj.pagetype) == "DPD" || v._2(UserObj.pagetype) == "QPD")
      .mapValues(x => (x(UserObj.productsku), x(UserObj.browserid), x(UserObj.domain)))
    var today = "_daily"
    var incremental = dailyIncremental.distinct()
      .map(x => (x._1.toString, x._2._1.toString, x._2._2.toString, x._2._3.toString))
      .toDF("userid" + today, "sku" + today, "device" + today, "domain" + today)
    return incremental
  }

  def ProcessSurf3Variable(mergedData: DataFrame, incremental: DataFrame): DataFrame ={
    var today = "_daily"
    var explodedMergedData = mergedData.explode("skuList", "sku") { str: List[String] => str.toList }
    var joinResult = incremental.join(explodedMergedData, incremental("userid" + today) === explodedMergedData("userid"))
      .where(incremental("sku" + today) === explodedMergedData("sku"))
      .select("userid", "sku", "device" + today, "domain" + today)
      .withColumnRenamed("device" + today, "device")
      .withColumnRenamed("domain" + today, "domain")
      .distinct
    return joinResult
  }

  def mergeSurf3Variable(hiveContext: HiveContext, mergedData: DataFrame, incremental: DataFrame, yesterDate:String): DataFrame = {
    import hiveContext.implicits._
    val format = new java.text.SimpleDateFormat("dd/MM/yyyy")
    val ft = new java.text.SimpleDateFormat("dd-MM-YYYY")
    var col = mergedData.columns
    var userid = 0
    var skuList = 0
    var dt = 0
    for (i <- 1 to (col.length - 1)) {
      if (col(i) == "userid")
        userid = i
      else if (col(i) == "skuList")
        skuList = i
      else if (col(i) == "dt")
        dt = i
    }
    val yesterMerge = mergedData.map(x => (x(userid), x(dt), x(skuList), TimeUtils.daysBetweenTwoDates(format.parse(yesterDate), format.parse(x(dt).toString)).toInt))
      .filter(x => x._4 < 29).map(x => (x._1.toString, x._2.toString, x._3.toString)).toDF("userid", "dt", "skuList")

    val IncrementalMerge = incremental.map(t => (t(0).toString, t(1).toString))
      .reduceByKey((x, y) => (x + "," + y))
      .map(v => (v._1, yesterDate.toString, (v._2.split(",").toSet.toList)))
      .toDF("userid", "dt", "skuList")
    return yesterMerge.unionAll(IncrementalMerge)
  }
    def variableSurf1(GroupedData: RDD[(Any, Row)], UserObj: GroupData): RDD[((String),String)] = {
      //def variableSurf1(GroupedData: RDD[(String, Row)], UserObj: GroupData): RDD[(String, Row)] = {
      val actualvisitid = UserObj.actualvisitid
      val productsku = UserObj.productsku
      val domain = UserObj.domain
      val browserid = UserObj.browserid
      val userid = UserObj.uid
      val a = GroupedData.filter((x => x._2(UserObj.productsku) != null))
      val b = a.map(x => ((if((x._2(userid)==null) && (x._2(domain).toString=="android"||x._2(domain).toString=="ios"||x._2(domain).toString=="windows"))"_app_"+x._2(browserid) else x._2(userid), x._2(actualvisitid), x._2(browserid), x._2(domain)).toString, x._2(productsku).toString))
      //map(x => ((if((x(uid)==null) && (x(domain).toString=="android" ||x(domain).toString=="ios" ||x(domain).toString=="windows" )) "_app_"+x(browserid).toString else x(uid)).toString,x))
      //val b=a.map(x=>((if ((x._2(UserObj.domain))=="w" || ((x._2(UserObj.domain))=="m")) x._2(UserObj.uid) else "_app_"+x._2(UserObj.browserid),x._2(UserObj.actualvisitid),x._2(UserObj.browserid),x._2(UserObj.domain)),x._2(UserObj.productsku)))
      val c = b.reduceByKey((x, y) => (x + "," + y))
      return c
    }

}
