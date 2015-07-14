package com.jabong.dap.model.clickstream.utils

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext


/**
 * Created by Divya on 13/7/15.
 */
object GetMergedClickstreamData extends java.io.Serializable {

  def mergeAppsWeb(hiveContext: HiveContext, tablename: String): DataFrame = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1);
    val mFormat = new SimpleDateFormat("MM")
    var year = cal.get(Calendar.YEAR);
    var day = cal.get(Calendar.DAY_OF_MONTH);
    var month = mFormat.format(cal.getTime());
    val pagevisit = hiveContext.sql("select * from " + tablename + " where browserid is not null and date1=" + day + " and month1=" + month + " and year1=" + year)
    return pagevisit
  }
}
