package com.jabong.dap.model.clickstream.variables

import java.text.SimpleDateFormat
import java.util.Calendar

import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.model.clickstream.utils.GroupData
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ SQLContext, DataFrame, Row }
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by Divya on 15/7/15.
 */
object GetSurfVariables extends java.io.Serializable {

  /**
   * For a customer(userid,device.domain) -> list of products viewd yesterday
   * @param GroupedData
   * @param UserObj
   * @param hiveContext
   * @return (DataFrame)
   */
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

  /**
   * List of [user, sku, device, domain]
   * user who have viewed same sku yesterday(daily Incremental) and in 2-30 days (mergedData)
   * @param mergedData
   * @param incremental
   * @return (DataFrame)
   */
  def ProcessSurf3Variable(mergedData: DataFrame, incremental: DataFrame): DataFrame = {
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

  /**
   * List of [user, date, List[Sku]] of last 29 days
   * @param mergedData
   * @param incremental
   * @param yesterDate
   * @return (DataFrame)
   */
  def mergeSurf3Variable(hiveContext: HiveContext, mergedData: DataFrame, incremental: DataFrame, yesterDate: String): DataFrame = {
    import hiveContext.implicits._

    val dateFormat = new SimpleDateFormat("dd/MM/yyyy")
    val cal = Calendar.getInstance()
    cal.setTime(dateFormat.parse(yesterDate))
    cal.add(Calendar.DATE, -29)
    var filterDate = dateFormat.format(cal.getTime())
    val IncrementalMerge = incremental.map(t => (t(0).toString, t(1).toString))
      .reduceByKey((x, y) => (x + "," + y))
      .map(v => (v._1, yesterDate.toString, (v._2.split(",").toSet.toList)))
      .toDF("userid", "dt", "skuList")

    if (mergedData != null) {
      val yesterMerge = mergedData.filter("dt != '" + filterDate.toString + "'")
      return yesterMerge.unionAll(IncrementalMerge)
    } else {
      return IncrementalMerge
    }
  }

  def uidToDeviceid(hiveContext: HiveContext): DataFrame = {
    val uiddeviceiddf = hiveContext.sql("select distinct case when userid is null then concat('_app_',bid) else userid end as userid,bid as deviceid,domain,max(pagets) as pagets from finalpagevisit where bid is not null and pagets is not null group by userid,bid,domain order by userid,pagets desc")
    return uiddeviceiddf
  }

  /**
   * Converts null app userid to _app_browserid and filter null desktop userid
   * Returns list of [userid, browserid,actualvisitid,domain, List[Sku]]
   * @param hiveContext
   * @return (DataFrame)
   */
  def listOfProductsViewedInSession(hiveContext: HiveContext, tablename: String): DataFrame = {
    val query = "select case when (domain in('android','ios','windows') and userid is null) then concat('_app_',browserid) else userid end as userid,browserid,actualvisitid,domain,collect_set(productsku) from " + tablename + " where browserid is not null and productsku is not null and pagetype in ('CPD','DPD','QPD') and (userid is not null and domain in ('w','m','android','ios','windows'))group by userid,browserid,actualvisitid,domain"
    val surf1Variables = hiveContext.sql(query)
    return surf1Variables
  }

}
