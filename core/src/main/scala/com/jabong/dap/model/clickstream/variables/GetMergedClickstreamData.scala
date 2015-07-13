package com.jabong.dap.model.clickstream.variables

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import java.util.Calendar
import java.text.SimpleDateFormat
import scala.collection.immutable.HashMap


/**
 * Created by Divya on 13/7/15.
 */
object GetMergedClickstreamData extends java.io.Serializable {

  def mergeAppsWeb(hiveContext: HiveContext, tablename: String): DataFrame = {
    val today = Calendar.getInstance()
    val mFormat = new SimpleDateFormat("MM")
    var year = today.get(Calendar.YEAR);
    var day = today.get(Calendar.DAY_OF_MONTH)-1;
    var month = mFormat.format(today.getTime());
    val pagevisit = hiveContext.sql("select * from " + tablename + " where bid is not null and date1=" + day + " and month1=" + month + " and year1=" + year)
    return pagevisit
  }
}
