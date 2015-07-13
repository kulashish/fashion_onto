package com.jabong.dap.model.clickstream.variables

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}


import scala.collection.immutable.HashMap

/**
 * Created by Divya on 13/7/15.
 */
class SurfVariables extends java.io.Serializable{

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Clickstream Surf Variables")
    val sc = new SparkContext(conf)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    val pagevisit: DataFrame = GetMergedClickstreamData.mergeAppsWeb(hiveContext, args(0))
    var UserObj = new GroupData(hiveContext, pagevisit)
    UserObj.calculateColumns()
    val userWiseData: RDD[(String, Row)] = UserObj.groupDataByUser()
    val browserWiseData: RDD[(String, Row)] = UserObj.groupDataByBrowser()

  }

  def coalesce(id1: Any,id2:Any): String ={
    if(id1 == null)
      return id2.toString
    else
      return id1.toString
  }

  //def fullName: String = ClickstreamConstants.database + "." + ClickstreamConstants.tablename

}

