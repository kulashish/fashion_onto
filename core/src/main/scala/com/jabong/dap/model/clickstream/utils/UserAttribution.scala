package com.jabong.dap.model.clickstream.utils

import com.jabong.dap.model.clickstream.schema.PagevisitSchema
import com.jabong.dap.model.clickstream.variables.VariableMethods._
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
import org.apache.spark.sql.hive.HiveContext


/**
 * Created by Divya
 */
class UserAttribution (hiveContext: HiveContext, sqlContext: SQLContext, pagevisit: DataFrame) extends java.io.Serializable {

  var pagets = 0
  var pagetype = 0
  var brand = 0
  var domain = 0
  var actualvisitid, visitts, uid, browserid, productsku, device = 0

  import hiveContext.implicits._
  def attribute(): DataFrame = {
    pagevisit.as('pagevisit)
    calculateColumns(pagevisit)
    val bg = pagevisit.map(x => (x(browserid).toString,List(Tuple2(x(pagets),(Array(x(uid),x(browserid),x(device),x(domain),x(pagetype),x(actualvisitid),x(visitts),x(productsku),x(brand)))))))
      .partitionBy(new org.apache.spark.HashPartitioner(400))
      .reduceByKey((x, y) => allocateUserToPreviousNull(x, y))
      .mapValues(x=> allocateUserToLaterNull(x) )
      .flatMap(_._2)
      .map(x=> Row(x._1,x._2(0),x._2(1),x._2(2),x._2(3),x._2(4),x._2(5),x._2(6),x._2(7),x._2(8)))
    val newDataFrame = sqlContext.createDataFrame(bg,PagevisitSchema.userAttribute)

    return newDataFrame
  }

  def allocateUserToPreviousNull(x: List[(Any, Array[Any])], y:List[(Any, Array[Any])]):List[(Any, Array[Any])] = {
    val merge = x ::: y
    val data =
      for (m <- 0 to merge.length - 1) yield new  TimeBasedSorter(merge(m)._1.toString, merge(m)._2)
    var sortedData = data sortWith comparePagets
    var cnt = 0
    var list: List[(Any, Array[Any])] = List()
    var previousUser:Any= ""
    for (b <- sortedData) {
      if (cnt == 0) {
          list.++=(List(b.pagets -> b.info))
          previousUser = b.info(0)
          cnt += 1
        } else if ((b.info(0) == null)) {
          b.info(0) = previousUser
          list.++=(List(b.pagets -> b.info))
          cnt += 1
        }
        else
        {
          list.++=(List(b.pagets -> b.info))
          previousUser = b.info(0).toString
        }
      }
    return list
  }

  def allocateUserToLaterNull(x: List[(Any, Array[Any])]):List[(Any, Array[Any])] = {
    val merge = x
    val data =
      for (m <- 0 to merge.length - 1) yield new  TimeBasedSorter(merge(m)._1.toString, merge(m)._2)
    var sortedData = data sortWith comparePagetsInDescending
    var cnt = 0
    var list: List[(Any, Array[Any])] = List()
    var previousUser:Any= ""
    for (b <- sortedData) {
      if (cnt == 0) {
        list.++=(List(b.pagets -> b.info))
        previousUser = b.info(0)
        cnt += 1
      } else if ((b.info(0) == null)) {
        b.info(0) = previousUser
        list.++=(List(b.pagets -> b.info))
        cnt += 1
      }
      else
      {
        list.++=(List(b.pagets -> b.info))
        previousUser = b.info(0).toString
      }
    }
    return list
  }

  def calculateColumns(pagevisit:DataFrame): Unit =
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
        domain = i
      else if(res(i) == "device")
        device = i
      else if(res(i) == "actualvisitid")
        actualvisitid = i
      else if(res(i) == "visitts")
        visitts = i
      else if(res(i) == "userid")
        uid = i
      else if(res(i) == "browserid")
        browserid = i
      else if(res(i) == "productsku")
        productsku = i
    }
  }
}
