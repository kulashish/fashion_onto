package com.jabong.dap.model.clickstream.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}

/**
 * Created by Divya on 13/7/15.
 *
 * This class groups the clickstream data user wise or Browserwise(where userid is not there and returns a RDD pair like [key= userid, value= row]
 *
 * The returned object/RDD is HashPartitioned and persisted by Key
 */

class GroupData (hiveContext: HiveContext, pagevisit: DataFrame) extends java.io.Serializable {
  var pagets = 0
  var pagetype = 0
  var brand = 0
  var domain = 0
  var actualvisitid, visitts, uid, browserid, productsku = 0

  def groupDataByUser(): RDD[(String, Row)] = {
    pagevisit.as('pagevisit)
    val ug:RDD[(String, Row)] = pagevisit.filter("pagets is not null and userid is not null").map(x => (x(uid).toString,x)).partitionBy(new org.apache.spark.HashPartitioner(32)).persist()
    return ug
  }

  def groupDataByBrowser(): RDD[(String, Row)] = {
    pagevisit.as('pagevisit)
    val br:RDD[(String, Row)] = pagevisit.filter("pagets is not null and userid is null").map(x => (x(browserid).toString,x)).partitionBy(new org.apache.spark.HashPartitioner(32)).persist()
    return br
  }

  def surfVariableData(): RDD[(Any, Row)] = {
    pagevisit.as('pagevisit)
    val br:RDD[(Any, Row)] = pagevisit.filter("pagets is not null").map(x => (x(uid),x)).partitionBy(new org.apache.spark.HashPartitioner(32)).persist()
      //map(x => ((if((x(uid)==null) && (x(domain).toString=="android" ||x(domain).toString=="ios" ||x(domain).toString=="windows" )) "_app_"+x(browserid).toString else x(uid)).toString,x)).partitionBy(new org.apache.spark.HashPartitioner(32)).persist()
    return br
  }

  def calculateColumns(): Unit =
  {
    val res = pagevisit.columns
    for (i <- 1 to (res.length - 1)) {
      if (res(i) == "pagetype")
        pagetype = i
      else if (res(i) == "pagets")
        pagets = i
      else if (res(i) == "brand")
        brand = i
      else if(res(i) == "domain")
        domain=i
      else if(res(i) == "actualvisitid")
        actualvisitid=i
      else if(res(i) == "visitts")
        visitts=i
      else if(res(i) == "userid")
        uid=i
      else if(res(i) == "browserid")
        browserid=i
      else if(res(i) == "productsku")
        productsku=i
    }

  }
}
