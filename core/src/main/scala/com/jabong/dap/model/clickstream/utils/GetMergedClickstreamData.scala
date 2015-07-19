package com.jabong.dap.model.clickstream.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext


/**
 * Created by Divya on 13/7/15.
 */
object GetMergedClickstreamData extends java.io.Serializable {

  def mergeAppsWeb(hiveContext: HiveContext, tablename: String, year: Int, day: Int, month: String): DataFrame = {

    val pagevisit = hiveContext.sql("select * from " + tablename + " where browserid is not null and date1=" + day + " and month1=" + month + " and year1=" + year)
    return pagevisit
  }
}
