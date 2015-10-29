package com.jabong.dap.model.clickstream.variables

import java.io.File

import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.PageVisitVariables
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.common.{OptionUtils, Spark}
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.PathBuilder
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.DataVerifier
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.clickstream.ClickStreamConstant
import com.jabong.dap.model.clickstream.utils.GroupData
import grizzled.slf4j.Logging
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.WrappedArray

/**
 * Created by Divya on 15/7/15.
 */
object GetSurfVariables extends java.io.Serializable with Logging {

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
      .toDF(PageVisitVariables.USER_ID + today, PageVisitVariables.SKU + today, PageVisitVariables.DEVICE + today, PageVisitVariables.DOMAIN + today)
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
      explodedMergedData = mergedData.explode(PageVisitVariables.SKU_LIST, PageVisitVariables.SKU) { str: WrappedArray[String] => str.toList }
      //explodedMergedData = mergedData.explode(PageVisitVariables.SKU_LIST, PageVisitVariables.SKU) { str: List[String] => str.toList }

      var joinResult = incremental.join(explodedMergedData, incremental(PageVisitVariables.USER_ID + today) === explodedMergedData(PageVisitVariables.USER_ID))
        .where(incremental(PageVisitVariables.SKU + today) === explodedMergedData(PageVisitVariables.SKU))
        .select(PageVisitVariables.USER_ID, PageVisitVariables.SKU, PageVisitVariables.DEVICE + today, PageVisitVariables.DOMAIN + today)
        .withColumnRenamed(PageVisitVariables.DEVICE + today, PageVisitVariables.DEVICE)
        .withColumnRenamed(PageVisitVariables.DOMAIN + today, PageVisitVariables.DOMAIN)
        .distinct
      return joinResult
    } else {
      var joinResult = incremental.select(PageVisitVariables.USER_ID + today, PageVisitVariables.SKU + today, PageVisitVariables.DEVICE + today, PageVisitVariables.DOMAIN + today)
        .withColumnRenamed(PageVisitVariables.USER_ID + today, PageVisitVariables.USER_ID)
        .withColumnRenamed(PageVisitVariables.SKU + today, PageVisitVariables.SKU)
        .withColumnRenamed(PageVisitVariables.DEVICE + today, PageVisitVariables.DEVICE)
        .withColumnRenamed(PageVisitVariables.DOMAIN + today, PageVisitVariables.DOMAIN)
        .distinct
      return joinResult
    }
  }

  /**
   * List of [user, date, List[Sku]] of last 29 days
   * @param mergedData
   * @param incremental
   * @param date
   * @return (DataFrame)
   */
  def mergeSurf3Variable(hiveContext: HiveContext, mergedData: DataFrame, incremental: DataFrame, date: String): DataFrame = {
    import hiveContext.implicits._

    val sqlContext = Spark.getSqlContext()
    // val dateFormat = new SimpleDateFormat("dd/MM/yyyy")
    // val cal = Calendar.getInstance()
    // cal.setTime(yesterDate)
    // cal.add(Calendar.DATE, -29)
    // var filterDate = dateFormat.format(cal.getTime())
    val filterDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(-29, TimeConstants.DATE_FORMAT_FOLDER, date), TimeConstants.DATE_FORMAT_FOLDER, "dd/MM/yyyy")

    incremental.select("userid_daily", "sku_daily").registerTempTable("tempincremental")

    var IncrementalMerge = hiveContext.sql("select userid_daily as userid,collect_list(sku_daily) as skuList from tempincremental group by userid_daily")
      .map(x => (x(0).toString, date.toString, x(1).asInstanceOf[WrappedArray[String]])).toDF(PageVisitVariables.USER_ID, "dt", PageVisitVariables.SKU_LIST)

    if (mergedData != null) {
      val yesterMerge = mergedData.filter("dt != '" + filterDate.toString + "'")
        .select(PageVisitVariables.USER_ID, "dt", PageVisitVariables.SKU_LIST)
      return yesterMerge.unionAll(IncrementalMerge)
    } else {
      return IncrementalMerge
    }

  }

  def getSurf3mergedForLast30Days(params: ParamInfo): DataFrame = {

    println("Start Time: " + TimeUtils.getTodayDate(TimeConstants.DATE_TIME_FORMAT_MS))
    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getTodayDate(TimeConstants.DATE_FORMAT_FOLDER))
    val saveMode = params.saveMode
    var tablename = ClickStreamConstant.MERGE_PAGEVISIT
    for (i <- 30 to 1 by -1) {
      val dateFolder = TimeUtils.getDateAfterNDays(-i, TimeConstants.DATE_FORMAT_FOLDER, incrDate)
      var dataPath = "/data/clickstream/merge" + File.separator + dateFolder

      val conf = new SparkConf().setAppName("Clickstream Surf Variables").set("spark.driver.allowMultipleContexts", "true")
      Spark.init(conf)
      val hiveContext = Spark.getHiveContext()

      val dayBeforeYesterdayDateFolder = TimeUtils.getDateAfterNDays(-i - 1, TimeConstants.DATE_FORMAT_FOLDER, incrDate)

      if (DataVerifier.dataExists(dataPath)) {

        val currentMergedDataPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.CLICKSTREAM, "Surf3mergedData30", DataSets.DAILY_MODE, dateFolder)
        if (DataWriter.canWrite(saveMode, currentMergedDataPath)) {
          var oldMergedDataPath = PathBuilder.buildPath(ConfigConstants.READ_OUTPUT_PATH, DataSets.CLICKSTREAM, "Surf3mergedData30", DataSets.DAILY_MODE, dayBeforeYesterdayDateFolder)

          var oldMergedData: DataFrame = null
          // check if merged data exists
          if (DataVerifier.dataExists(oldMergedDataPath)) {
            oldMergedData = hiveContext.read.load(oldMergedDataPath)
          }
          var useridDeviceidFrame: DataFrame = SurfVariablesMain.getAppIdUserIdData(dateFolder, ClickStreamConstant.MERGE_PAGEVISIT)

          var UserObj = new GroupData()
          UserObj.calculateColumns(useridDeviceidFrame)
          val userWiseData: RDD[(String, Row)] = UserObj.groupDataByAppUser(hiveContext, useridDeviceidFrame)
          var incremental = GetSurfVariables.Surf3Incremental(userWiseData, UserObj, hiveContext)

          var mergedData = GetSurfVariables.mergeSurf3Variable(hiveContext, oldMergedData, incremental, dateFolder)

          DataWriter.writeParquet(mergedData, currentMergedDataPath, saveMode)
        }

      } else {

        val currentMergedDataPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.CLICKSTREAM, "Surf3mergedData30", DataSets.DAILY_MODE, dateFolder)
        if (DataWriter.canWrite(saveMode, currentMergedDataPath)) {
          var oldMergedDataPath = PathBuilder.buildPath(ConfigConstants.READ_OUTPUT_PATH, DataSets.CLICKSTREAM, "Surf3mergedData30", DataSets.DAILY_MODE, dayBeforeYesterdayDateFolder)

          var oldMergedData: DataFrame = null
          // check if merged data exists
          if (DataVerifier.dataExists(oldMergedDataPath)) {
            oldMergedData = hiveContext.read.load(oldMergedDataPath)
            DataWriter.writeParquet(oldMergedData, currentMergedDataPath, saveMode)
          }
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
  def listOfProductsViewedInSession(hiveContext: HiveContext, tablename: String, year: Int, day: Int, month: Int): DataFrame = {
    val query = "select " +
      "case when (domain in('android','ios','windows') and userid is null) then concat('_app_',browserid) else userid end as userid, " +
      "browserid, actualvisitid, domain, collect_list(productsku) as skuList from " + tablename +
      " where year1=" + year + " and month1=" + month + " and date1=" + day + " and browserid is not null and productsku is not null and pagetype in ('CPD','DPD','QPD') and (userid is not null and domain in ('w','m') or domain in ('android','ios','windows'))" +
      "group by userid, browserid, actualvisitid, domain"
    val surf1Variables = hiveContext.sql(query)
    surf1Variables
  }

}
