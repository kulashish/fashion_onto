package com.jabong.dap.model.clickstream.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by Divya on 13/7/15.
 */

object GetMergedClickstreamData extends java.io.Serializable {

  def mergeAppsWeb(hiveContext: HiveContext, tablename: String, year: Int, day: Int, month: Int): DataFrame = {
    val pagevisit = hiveContext.sql("select userid, browserid, pagetype, device, domain,  pagets, actualvisitid, visitts, productsku, brand, add4push from " + tablename + " where browserid is not null and pagets is not null and (userid != 'CH2' or userid is null) and date1=" + day + " and month1=" + month + " and year1=" + year)
    return pagevisit
  }

}
