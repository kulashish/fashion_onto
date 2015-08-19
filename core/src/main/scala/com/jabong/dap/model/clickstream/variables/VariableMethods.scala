package com.jabong.dap.model.clickstream.variables

import java.text.SimpleDateFormat

import com.jabong.dap.common.time.TimeConstants
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.clickstream.utils.GroupData
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{ DataFrame, Row }

import scala.collection.mutable.{ Map, Set }
import scala.util.control._

/**
 * Created by Divya on 26/5/15.
 *
 * Methods in this object calculate/process one variable each
 * Method name depicts the variable being calculated
 */

object VariableMethods extends java.io.Serializable {

  def appCheck(GroupedData: RDD[(String, Row)], pagevisit: DataFrame, UserObj: GroupData, hiveContext: HiveContext): Unit = {
    val domain = UserObj.domain
    val userDomain = GroupedData.mapValues(x => scala.collection.mutable.Set(x(domain).toString))
    val appcheck = userDomain.reduceByKey((x, y) => appCheckReducer(x, y))
    val result = appcheck.mapValues(x => x(DataSets.DESKTOP))
    //  result.saveAsTextFile(ClickstreamConstants.appcheckOutputPath)
    println(result.count())
  }

  def appCheckReducer(x: Set[String], y: Set[String]): Set[String] = {
    return x ++ y
  }

  def lastBrowsedDevice(GroupedData: RDD[(String, Row)], pagevisit: DataFrame): RDD[(String, Any)] = {
    val col = pagevisit.columns
    var pagets: Int = 0
    for (i <- 1 to (col.length - 1)) {
      if (col(i) == "pagets")
        pagets = i
    }
    var device: Int = 0
    for (i <- 1 to (col.length - 1)) {
      if (col(i) == "device")
        device = i
    }
    val userPagets = GroupedData.mapValues(x => (x(pagets).toString, x(device)))
    val lastDevice = userPagets.reduceByKey((x, y) => lastDeviceReducer(x, y)).map(x => (x._1, x._2._2))
    return lastDevice
  }

  def lastDeviceReducer(x: (String, Any), y: (String, Any)): (String, Any) = {

    val format = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT_MS)
    val date1 = format.parse(x._1)
    val date2 = format.parse(y._1)
    if (date1 after date2) {
      return x
    } else
      return y
  }

  def getBrands(brands: Tuple2[String, List[(Any, Array[Any])]]): Array[String] =
    {
      var cnt = 0
      var arr = new Array[String](4)
      arr(cnt) = brands._1.toString
      for (br <- brands._2) {
        cnt += 1
        arr(cnt) = br._2(1).toString
      }
      arr
    }

  def comparePagets(a: TimeBasedSorter, b: TimeBasedSorter): Boolean = {
    val a1: String = a.pagets.toString
    val b1: String = b.pagets.toString
    val format = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT_MS)
    val date1 = format.parse(a1)
    val date2 = format.parse(b1)
    if (date1 after date2) {
      true
    } else
      false
  }

  def comparePagetsInDescending(a: TimeBasedSorter, b: TimeBasedSorter): Boolean = {
    val a1: String = a.pagets.toString
    val b1: String = b.pagets.toString
    val format = new SimpleDateFormat(TimeConstants.DATE_TIME_FORMAT_MS)
    val date1 = format.parse(a1)
    val date2 = format.parse(b1)
    if (date1 before date2) {
      true
    } else
      false
  }

  def returnLast3Reducer(x: List[(Any, Array[Any])], y: List[(Any, Array[Any])]): List[(Any, Array[Any])] = {
    val merge = x ::: y
    val brand =
      for (m <- 0 to merge.length - 1) yield new TimeBasedSorter(merge(m)._1.toString, merge(m)._2)
    var lastBrands = brand sortWith comparePagets
    // Get last three brands
    var cnt = 0
    var list: List[(Any, Array[Any])] = List()
    val loop = new Breaks
    var previousBrand1 = ""
    var previousBrand2 = ""
    loop.breakable {
      for (b <- lastBrands) {
        if (cnt == 0) {
          list.++=(List(b.pagets -> b.info))
          previousBrand1 = b.info(1).toString
          cnt += 1
        } else if ((previousBrand1 != b.info(1).toString) && (previousBrand2 != b.info(1).toString)) {
          list.++=(List(b.pagets -> b.info))
          previousBrand2 != b.info(1).toString
          cnt += 1
        }

        if (cnt == 3) loop.break()
      }
    }
    list
  }

  def lastBrandsViewed(GroupedData: RDD[(String, Row)], pagevisit: DataFrame, UserObj: GroupData): RDD[(String, String, String, String)] = {

    val pagets = UserObj.pagets
    val br = UserObj.brand
    val pagetype = UserObj.pagetype

    val userList = GroupedData.filter(x => x._2(pagets) != null).mapValues(x => List(x(pagets) -> Array(x(pagetype), x(br))))
    val ProductViews = userList.mapValues(x => x.filter(v => v._2(0) == "CPD" || v._2(0) == "DPD" || v._2(0) == "QPD"))
    val TimeWiseData = ProductViews.filter { case (k, v) => !v.isEmpty }
    val userBrand: RDD[(String, List[(Any, Array[Any])])] = TimeWiseData.reduceByKey((x, y) => returnLast3Reducer(x, y))
    val brands = userBrand.map(x => getBrands(x)).map(r => (r(0), r(1), r(2), r(3)))
    return brands
  }

  def lastDomain(GroupedData: RDD[(String, Row)], pagevisit: DataFrame, hiveContext: HiveContext): Unit = {
    val col = pagevisit.columns
    var visitts: Int = 0
    var domain: Int = 0
    for (i <- 1 to (col.length - 1)) {
      if (col(i) == "visitts")
        visitts = i
      if (col(i) == "domain")
        domain = i
    }

    val userLastDomain = GroupedData
      //.mapValues(x => (x.(domain).toString, x.(visitts).toString))
      .map(x => (x._1, Map(x._2(domain).toString -> x._2(visitts).toString)))
      .reduceByKey((x, y) => maxByDate(x, y))
      .map(x => (x._1, getDomain(x._2.keys.take(1).head)))
    // .saveAsTextFile(ClickstreamConstants.lastDomainOutputPath)
  }

  def maxByDate(x: Map[String, String], y: Map[String, String]): Map[String, String] = {
    val xy: Map[String, String] = x ++ y
    (Map(xy.maxBy(_._2)))
    // keysIterator.reduceLeft((x,y) => if (x > y) x else y)
  }

  def getDomain(x: String): String = {
    var domain: String = ""
    if (x == DataSets.DESKTOP || x == DataSets.MOBILEWEB) {
      domain = DataSets.WEB
    } else {
      domain = DataSets.APP
    }
    domain
  }

  class TimeBasedSorter(
      val pagets: String,
      val info: Array[Any]) {
    override def toString() =
      "(" + pagets + ") " + info
  }

}