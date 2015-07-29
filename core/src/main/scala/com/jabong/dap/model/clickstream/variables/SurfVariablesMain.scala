package com.jabong.dap.model.clickstream.variables

import java.text.SimpleDateFormat
import java.util.Calendar
import javax.xml.crypto.Data

import com.jabong.dap.common.Spark
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.read.PathBuilder
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.clickstream.utils.{UserAttribution, GetMergedClickstreamData, GroupData}
import com.jabong.dap.data.storage.merge.common.DataVerifier
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row }

/**
 * Created by Divya on 13/7/15.
 */
object SurfVariablesMain extends java.io.Serializable {

  def main(args: Array[String]) {
    var gap = 5
    val conf = new SparkConf().setAppName("Clickstream Surf Variables").set("spark.driver.allowMultipleContexts", "true")
    Spark.init(conf)
    val hiveContext = Spark.getHiveContext()
    val sqlContext = Spark.getSqlContext()
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -gap)
    val mFormat = new SimpleDateFormat("MM")
    var year = cal.get(Calendar.YEAR)
    var day = cal.get(Calendar.DAY_OF_MONTH)
    var month = mFormat.format(cal.getTime())
    val dateFormat = new SimpleDateFormat("dd/MM/YYYY")
    var dt = dateFormat.format(cal.getTime())
    val tablename = args(0)
    val finalTempTable = "finalpagevisit"

    val currentMergedDataPath = args(1) + "/" + year + "/" + month + "/" + day + "/Surf3mergedData"
    var processedVariablePath = args(2) + "/" + year + "/" + month + "/" + day + "/Surf3ProcessedVariable"
    val userDeviceMapPath = args(2) + "/" + year + "/" + month + "/" + day + "/userDeviceMap"
    var surf1VariablePath = args(2) + "/" + year + "/" + month + "/" + day + "/Surf1ProcessedVariable"

    cal.add(Calendar.DATE, -1)
    year = cal.get(Calendar.YEAR)
    day = cal.get(Calendar.DAY_OF_MONTH)
    month = mFormat.format(cal.getTime())
    var oldMergedDataPath = args(1) + "/" + year + "/" + month + "/" + day + "/Surf3mergedData"
    var oldMergedData = sqlContext.read.load(oldMergedDataPath)
    val today = "_daily"

    var useridDeviceidFrame = getAppIdUserIdData(cal, tablename)
    var UserObj = new GroupData()
    UserObj.calculateColumns(useridDeviceidFrame)
    val userWiseData: RDD[(String, Row)] = UserObj.groupDataByAppUser(useridDeviceidFrame)

    var incremental = GetSurfVariables.Surf3Incremental(userWiseData, UserObj, hiveContext)
    var processedVariable = GetSurfVariables.ProcessSurf3Variable(oldMergedData, incremental)
    var mergedData = GetSurfVariables.mergeSurf3Variable(hiveContext, oldMergedData, incremental, dt)
    mergedData.repartition(300).write.save(currentMergedDataPath)
    processedVariable.repartition(300).write.save(processedVariablePath)

    // user device mapping
    var userDeviceMapping = UserDeviceMapping
      .getUserDeviceMapApp(useridDeviceidFrame)
      .write.mode("error")
      .save(userDeviceMapPath)

    val variableSurf1 = GetSurfVariables.listOfProductsViewedInSession(hiveContext, finalTempTable)
    variableSurf1.write.save(surf1VariablePath)

  }

  def startClickstreamYesterdaySessionVariables() = {
    var gap = 1
    val hiveContext = Spark.getHiveContext()

    // calculate yesterday date
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -gap);
    val dateFormat = new SimpleDateFormat("dd/MM/YYYY")
    var dt = dateFormat.format(cal.getTime())

    val tablename = "merge.merge_pagevisit"
    val finalTempTable = "finalpagevisit"

    val yesterdayDate = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT)

    val userDeviceMapPath = PathBuilder.buildPath(DataSets.OUTPUT_PATH, "clickstream", DataSets.USER_DEVICE_MAP_APP, "daily", yesterdayDate)
    var surf1VariablePath = PathBuilder.buildPath(DataSets.OUTPUT_PATH, "clickstream", "Surf1ProcessedVariable", "daily", yesterdayDate)


    var useridDeviceidFrame = getAppIdUserIdData(cal, tablename)
    var UserObj = new GroupData()
    UserObj.calculateColumns(useridDeviceidFrame)

    // user device mapping
    var userDeviceMapping = UserDeviceMapping
      .getUserDeviceMapApp(useridDeviceidFrame)
      .write.mode("error")
      .save(userDeviceMapPath)

    // variable 1
    val variableSurf1 = GetSurfVariables.listOfProductsViewedInSession(hiveContext, finalTempTable)
    variableSurf1.write.save(surf1VariablePath)
  }

  def startSurf3Variable() = {
    var gap = 1
    val hiveContext = Spark.getHiveContext()

    // calculate yesterday date
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -gap);
    val tablename = "merge.merge_pagevisit"

    val dateFormat = new SimpleDateFormat("dd/MM/YYYY")
    var dt = dateFormat.format(cal.getTime())

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

    var useridDeviceidFrame = getAppIdUserIdData(cal, tablename)
    var UserObj = new GroupData()
    UserObj.calculateColumns(useridDeviceidFrame)
    val userWiseData: RDD[(String, Row)] = UserObj.groupDataByAppUser(useridDeviceidFrame)
    var incremental = GetSurfVariables.Surf3Incremental(userWiseData, UserObj, hiveContext)

    if (null != oldMergedData) {
      var processedVariable = GetSurfVariables.ProcessSurf3Variable(oldMergedData, incremental)
      processedVariable.write.save(processedVariablePath)
    }
    var mergedData = GetSurfVariables.mergeSurf3Variable(hiveContext, oldMergedData, incremental, dt)
    mergedData.write.parquet(currentMergedDataPath)


  }

  def getAppIdUserIdData(cal: Calendar, tablename: String): DataFrame = {
    val hiveContext = Spark.getHiveContext()
    val sqlContext = Spark.getSqlContext()

    val mFormat = new SimpleDateFormat("MM")
    val year = cal.get(Calendar.YEAR);
    val day = cal.get(Calendar.DAY_OF_MONTH);
    val month = mFormat.format(cal.getTime());

    val pagevisit: DataFrame = GetMergedClickstreamData.mergeAppsWeb(hiveContext, tablename, year, day, month)

    var attributeObj:UserAttribution = new UserAttribution(hiveContext, sqlContext, pagevisit)
    var userAttributedData: DataFrame = attributeObj.attribute()

    var UserObj = new GroupData()
    var useridDeviceidFrame = UserObj.appuseridCreation(userAttributedData)
    return useridDeviceidFrame
  }
}
