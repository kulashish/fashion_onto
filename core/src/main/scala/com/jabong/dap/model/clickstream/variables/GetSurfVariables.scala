package com.jabong.dap.model.clickstream.variables

import java.text.SimpleDateFormat
import java.util.Calendar

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.read.PathBuilder
import com.jabong.dap.data.storage.merge.common.DataVerifier
import com.jabong.dap.model.clickstream.utils.GroupData
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{ DataFrame, Row }

import scala.collection.mutable.ArrayBuffer

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
    var explodedMergedData: DataFrame = null
    if (mergedData != null) {
      explodedMergedData = mergedData.explode("skuList", "sku") { str: ArrayBuffer[String] => str.toList }
      //explodedMergedData = mergedData.explode("skuList", "sku") { str: List[String] => str.toList }

      var joinResult = incremental.join(explodedMergedData, incremental("userid" + today) === explodedMergedData("userid"))
        .where(incremental("sku" + today) === explodedMergedData("sku"))
        .select("userid", "sku", "device" + today, "domain" + today)
        .withColumnRenamed("device" + today, "device")
        .withColumnRenamed("domain" + today, "domain")
        .distinct
      return joinResult
    } else {
      var joinResult = incremental.select("userid" + today, "sku" + today, "device" + today, "domain" + today)
        .withColumnRenamed("userid" + today, "userid")
        .withColumnRenamed("sku" + today, "sku")
        .withColumnRenamed("device" + today, "device")
        .withColumnRenamed("domain" + today, "domain")
        .distinct
      return joinResult
    }
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

    val sqlContext = Spark.getSqlContext()
    val dateFormat = new SimpleDateFormat("dd/MM/yyyy")
    val cal = Calendar.getInstance()
    cal.setTime(dateFormat.parse(yesterDate))
    cal.add(Calendar.DATE, -29)
    var filterDate = dateFormat.format(cal.getTime())
    incremental.select("userid_daily", "sku_daily").registerTempTable("tempincremental")

    var IncrementalMerge = hiveContext.sql("select userid_daily as userid,collect_list(sku_daily) as skuList from tempincremental group by userid_daily")
      .map(x => (x(0).toString, yesterDate.toString, x(1).asInstanceOf[ArrayBuffer[String]])).toDF("userid", "dt", "skuList")

    if (mergedData != null) {
      val yesterMerge = mergedData.filter("dt != '" + filterDate.toString + "'")
        .select("userid", "dt", "skuList")
      return yesterMerge.unionAll(IncrementalMerge)
    } else {
      return IncrementalMerge
    }

  }

  def getSurf3mergedForLast30Days(): DataFrame =
    {
      var tablename = "merge.merge_pagevisit"
      for (i <- 30 to 1 by -1) {
        val cal = Calendar.getInstance()
        cal.add(Calendar.DATE, -i)
        var year = cal.get(Calendar.YEAR);
        var day = cal.get(Calendar.DAY_OF_MONTH);
        var month = cal.get(Calendar.MONTH);
        var dataPath = "/data/clickstream/merge" + "/" + year + "/" + month.toInt + 1 + "/" + day

        val conf = new SparkConf().setAppName("Clickstream Surf Variables").set("spark.driver.allowMultipleContexts", "true")
        Spark.init(conf)
        val hiveContext = Spark.getHiveContext()
        val sqlContext = Spark.getSqlContext()

        if (DataVerifier.dataExists(dataPath)) {

          val dateFormat = new SimpleDateFormat("dd/MM/YYYY")
          var dt = dateFormat.format(cal.getTime())

          val yesterdayDate = TimeUtils.getDateAfterNDays(-i, TimeConstants.DATE_FORMAT)
          val dayBeforeYesterdayDate = TimeUtils.getDateAfterNDays(-i - 1, TimeConstants.DATE_FORMAT)

          val currentMergedDataPath = PathBuilder.buildPath(ConfigConstants.OUTPUT_PATH, "clickstream", "Surf3mergedData30", "daily", yesterdayDate)
          var oldMergedDataPath = PathBuilder.buildPath(ConfigConstants.OUTPUT_PATH, "clickstream", "Surf3mergedData30", "daily", dayBeforeYesterdayDate)

          var oldMergedData: DataFrame = null
          // check if merged data exists
          if (DataVerifier.dataExists(oldMergedDataPath)) {
            oldMergedData = hiveContext.read.load(oldMergedDataPath)
          }
          var useridDeviceidFrame: DataFrame = SurfVariablesMain.getAppIdUserIdData(cal, "merge.merge_pagevisit")

          var UserObj = new GroupData()
          UserObj.calculateColumns(useridDeviceidFrame)
          val userWiseData: RDD[(String, Row)] = UserObj.groupDataByAppUser(hiveContext, useridDeviceidFrame)
          var incremental = GetSurfVariables.Surf3Incremental(userWiseData, UserObj, hiveContext)

          var mergedData = GetSurfVariables.mergeSurf3Variable(hiveContext, oldMergedData, incremental, dt)
          mergedData.write.parquet(currentMergedDataPath)

        } else {
          val dateFormat = new SimpleDateFormat("dd/MM/YYYY")
          var dt = dateFormat.format(cal.getTime())

          val yesterdayDate = TimeUtils.getDateAfterNDays(-i, TimeConstants.DATE_FORMAT)
          val dayBeforeYesterdayDate = TimeUtils.getDateAfterNDays(-i - 1, TimeConstants.DATE_FORMAT)

          val currentMergedDataPath = PathBuilder.buildPath(ConfigConstants.OUTPUT_PATH, "clickstream", "Surf3mergedData30", "daily", yesterdayDate)
          var oldMergedDataPath = PathBuilder.buildPath(ConfigConstants.OUTPUT_PATH, "clickstream", "Surf3mergedData30", "daily", dayBeforeYesterdayDate)

          var oldMergedData: DataFrame = null
          // check if merged data exists
          if (DataVerifier.dataExists(oldMergedDataPath)) {
            oldMergedData = hiveContext.read.load(oldMergedDataPath)
            oldMergedData.write.parquet(currentMergedDataPath)
          }
        }
      }
      return null

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
  def listOfProductsViewedInSession(hiveContext: HiveContext, tablename: String, year: Int, day: Int, month: String): DataFrame = {
    val query = "select case when (domain in('android','ios','windows') and userid is null) then concat('_app_',browserid) else userid end as userid,browserid,actualvisitid,domain,collect_list(productsku) from " + tablename + " where year1=" + year + " and month1=" + month + " and date1=" + day + " and browserid is not null and productsku is not null and pagetype in ('CPD','DPD','QPD') and (userid is not null and domain in ('w','m') or domain in ('android','ios','windows'))group by userid,browserid,actualvisitid,domain"
    val surf1Variables = hiveContext.sql(query)
    return surf1Variables
  }

}
