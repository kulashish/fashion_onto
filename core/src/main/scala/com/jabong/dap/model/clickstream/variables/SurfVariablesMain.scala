package com.jabong.dap.model.clickstream.variables

import java.text.SimpleDateFormat
import java.util.Calendar

import com.jabong.dap.common.Spark
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.read.{ DataVerifier, PathBuilder }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.clickstream.utils.{ GetMergedClickstreamData, GroupData }
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row }

/**
 * Created by Divya on 13/7/15.
 */
object SurfVariablesMain extends java.io.Serializable {

  def main(args: Array[String]) {
    var gap = 1
    val conf = new SparkConf().setAppName("Clickstream Surf Variables").set("spark.driver.allowMultipleContexts", "true")
    Spark.init(conf)
    val hiveContext = Spark.getHiveContext()
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -gap);
    val mFormat = new SimpleDateFormat("MM")
    var year = cal.get(Calendar.YEAR);
    var day = cal.get(Calendar.DAY_OF_MONTH);
    var month = mFormat.format(cal.getTime());
    val dateFormat = new SimpleDateFormat("dd/MM/YYYY")
    var dt = dateFormat.format(cal.getTime())
    val tablename = args(0)
    val pagevisit: DataFrame = GetMergedClickstreamData.mergeAppsWeb(hiveContext, tablename, year, day, month)
    val currentMergedDataPath = args(1) + "/" + year + "/" + month + "/" + day + "/Surf3mergedData"
    var processedVariablePath = args(2) + "/" + year + "/" + month + "/" + day + "/Surf3ProcessedVariable"
    val userDeviceMapPath = args(2) + "/" + year + "/" + month + "/" + day + "/userDeviceMap"
    var surf1VariablePath = args(3) + "/" + year + "/" + month + "/" + day + "/Surf1ProcessedVariable"
    cal.add(Calendar.DATE, -1);
    year = cal.get(Calendar.YEAR);
    day = cal.get(Calendar.DAY_OF_MONTH);
    month = mFormat.format(cal.getTime());
    var oldMergedDataPath = args(1) + "/" + year + "/" + month + "/" + day + "/Surf3mergedData"
    var sqlContext = Spark.getSqlContext()
    var oldMergedData = hiveContext.parquetFile(oldMergedDataPath)
    val today = "_daily"
    var UserObj = new GroupData(hiveContext, pagevisit)
    var useridDeviceidFrame = UserObj.appuseridCreation()
    UserObj.calculateColumns(useridDeviceidFrame)
    val userWiseData: RDD[(String, Row)] = UserObj.groupDataByAppUser(useridDeviceidFrame)
    var incremental = GetSurfVariables.Surf3Incremental(userWiseData, UserObj, hiveContext)
    var processedVariable = GetSurfVariables.ProcessSurf3Variable(oldMergedData, incremental)
    var mergedData = GetSurfVariables.mergeSurf3Variable(hiveContext, oldMergedData, incremental, dt)
    mergedData.saveAsParquetFile(currentMergedDataPath)
    incremental.save(processedVariablePath)

    // user device mapping
    var userDeviceMapping = UserDeviceMapping
      .getUserDeviceMapApp(useridDeviceidFrame)
      .write.mode("error")
      .save(userDeviceMapPath)
    val variableSurf1 = GetSurfVariables.listOfProductsViewedInSession(hiveContext, args(0))
    variableSurf1.write.save(surf1VariablePath)
  }

  def startClickstreamYesterdaySessionVariables() = {
    var gap = 1
    val hiveContext = Spark.getHiveContext()

    // calculate yesterday date
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -gap);
    val mFormat = new SimpleDateFormat("MM")
    val year = cal.get(Calendar.YEAR);
    val day = cal.get(Calendar.DAY_OF_MONTH);
    val month = mFormat.format(cal.getTime());
    val dateFormat = new SimpleDateFormat("dd/MM/YYYY")
    var dt = dateFormat.format(cal.getTime())

    val tablename = "merge.merge_pagevisit"
    val pagevisit: DataFrame = GetMergedClickstreamData.mergeAppsWeb(hiveContext, tablename, year, day, month)

    val yesterdayDate = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT)

    val userDeviceMapPath = PathBuilder.buildPath(DataSets.OUTPUT_PATH, "clickstream", "userDeviceMap", "daily", yesterdayDate)
    var surf1VariablePath = PathBuilder.buildPath(DataSets.OUTPUT_PATH, "clickstream", "Surf1ProcessedVariable", "daily", yesterdayDate)

    var UserObj = new GroupData(hiveContext, pagevisit)
    var useridDeviceidFrame = UserObj.appuseridCreation()
    UserObj.calculateColumns(useridDeviceidFrame)

    // user device mapping
    var userDeviceMapping = UserDeviceMapping
      .getUserDeviceMapApp(useridDeviceidFrame)
      .write.mode("error")
      .save(userDeviceMapPath)

    // variable 1
    val variableSurf1 = GetSurfVariables.listOfProductsViewedInSession(hiveContext, tablename)
    variableSurf1.write.save(surf1VariablePath)
  }

  def startSurf3Variable() = {
    var gap = 1
    val hiveContext = Spark.getHiveContext()

    // calculate yesterday date
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -gap);
    val mFormat = new SimpleDateFormat("MM")
    val year = cal.get(Calendar.YEAR);
    val day = cal.get(Calendar.DAY_OF_MONTH);
    val month = mFormat.format(cal.getTime());
    val dateFormat = new SimpleDateFormat("dd/MM/YYYY")
    var dt = dateFormat.format(cal.getTime())

    val tablename = "merge.merge_pagevisit"
    val pagevisit: DataFrame = GetMergedClickstreamData.mergeAppsWeb(hiveContext, tablename, year, day, month)

    val yesterdayDate = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT)
    val dayBeforeYesterdayDate = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT)

    val currentMergedDataPath = PathBuilder.buildPath(DataSets.OUTPUT_PATH, "clickstream", "Surf3mergedData", "daily", yesterdayDate)
    var processedVariablePath = PathBuilder.buildPath(DataSets.OUTPUT_PATH, "clickstream", "Surf3ProcessedVariable", "daily", yesterdayDate)

    var oldMergedDataPath = PathBuilder.buildPath(DataSets.OUTPUT_PATH, "clickstream", "Surf3mergedData", "daily", dayBeforeYesterdayDate)

    var oldMergedData: DataFrame = null

    // check if merged data exists
    if (DataVerifier.dataExists(oldMergedDataPath)) {
      oldMergedData = hiveContext.parquetFile(oldMergedDataPath)
    }

    var UserObj = new GroupData(hiveContext, pagevisit)
    var useridDeviceidFrame = UserObj.appuseridCreation()
    UserObj.calculateColumns(useridDeviceidFrame)
    val userWiseData: RDD[(String, Row)] = UserObj.groupDataByAppUser(useridDeviceidFrame)
    var incremental = GetSurfVariables.Surf3Incremental(userWiseData, UserObj, hiveContext)

    if (null != oldMergedData) {
      var processedVariable = GetSurfVariables.ProcessSurf3Variable(oldMergedData, incremental)
    }
    var mergedData = GetSurfVariables.mergeSurf3Variable(hiveContext, oldMergedData, incremental, dt)
    mergedData.saveAsParquetFile(currentMergedDataPath)
    incremental.save(processedVariablePath)

  }
}