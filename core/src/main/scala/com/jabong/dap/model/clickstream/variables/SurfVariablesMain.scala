package com.jabong.dap.model.clickstream.variables

import java.io.File

import com.jabong.dap.common.{OptionUtils, Spark}
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.acq.common.{ParamInfo, ParamJobInfo, ParamJobConfig}
import com.jabong.dap.data.read.PathBuilder
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.DataVerifier
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.clickstream.utils.{ GetMergedClickstreamData, GroupData }
import com.jabong.dap.model.custorder.ParamJsonValidator
import grizzled.slf4j.Logging
import net.liftweb.json.JsonParser.ParseException
import net.liftweb.json._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Row }

/**
 * Created by Divya on 13/7/15.
 */
object SurfVariablesMain extends java.io.Serializable with Logging {

  def main(args: Array[String]) {
    val gap = args(3).toInt
    val conf = new SparkConf().setAppName("Clickstream Surf Variables").set("spark.driver.allowMultipleContexts", "true")
    Spark.init(conf)
    val hiveContext = Spark.getHiveContext()
    val sqlContext = Spark.getSqlContext()
    // val cal = Calendar.getInstance()
    // cal.add(Calendar.DATE, -gap)
    // val mFormat = new SimpleDateFormat("MM")
    // var year = cal.get(Calendar.YEAR)
    // var day = cal.get(Calendar.DAY_OF_MONTH)
    // var month = mFormat.format(cal.getTime())
    // val dateFormat = new SimpleDateFormat("dd/MM/YYYY")
    // var dt = dateFormat.format(cal.getTime())
    val tablename = args(0)
    val finalTempTable = "finalpagevisit"

    // val dateFolder = File.separator + year + File.separator + month + File.separator + day
    val dateFolder = TimeUtils.getDateAfterNDays(-gap, TimeConstants.DATE_FORMAT_FOLDER)

    val currentMergedDataPath = args(1) + dateFolder + "/Surf3mergedData"
    val processedVariablePath = args(2) + dateFolder + "/Surf3ProcessedVariable"
    val userDeviceMapPath = args(2) + dateFolder + "/userDeviceMap"
    val surf1VariablePath = args(2) + dateFolder + "/Surf1ProcessedVariable"

    val useridDeviceidFrame = getAppIdUserIdData(dateFolder, tablename)
    val UserObj = new GroupData()
    UserObj.calculateColumns(useridDeviceidFrame)
    val userWiseData: RDD[(String, Row)] = UserObj.groupDataByAppUser(hiveContext, useridDeviceidFrame)

    // cal.add(Calendar.DATE, -1)
    // year = cal.get(Calendar.YEAR)
    // day = cal.get(Calendar.DAY_OF_MONTH)
    // month = mFormat.format(cal.getTime())
    val dayBeforeYesterdayDateFolder = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER, dateFolder)
    // var oldMergedDataPath = args(1) + File.separator + year + File.separator + month + File.separator + day + File.separator + "Surf3mergedData"
    val oldMergedDataPath = args(1) + File.separator + dayBeforeYesterdayDateFolder + File.separator + "Surf3mergedData"
    var oldMergedData: DataFrame = null
    if (DataVerifier.dataExists(oldMergedDataPath)) {
      oldMergedData = sqlContext.read.load(oldMergedDataPath)
    }
    val today = "_daily"
    val incremental = GetSurfVariables.Surf3Incremental(userWiseData, UserObj, hiveContext)
    val processedVariable = GetSurfVariables.ProcessSurf3Variable(oldMergedData, incremental)
    val mergedData = GetSurfVariables.mergeSurf3Variable(hiveContext, oldMergedData, incremental, dayBeforeYesterdayDateFolder)
    mergedData.write.save(currentMergedDataPath)
    processedVariable.write.save(processedVariablePath)

    // user device mapping
    /* var userDeviceMapping = UserDeviceMapping
      .getUserDeviceMapApp(useridDeviceidFrame)
      .write.mode("error")
      .save(userDeviceMapPath)

    val variableSurf1 = GetSurfVariables.listOfProductsViewedInSession(hiveContext, args(0), year, day, month)
    variableSurf1.write.save(surf1VariablePath)
*/
  }

  def startClickstreamYesterdaySessionVariables(params: ParamInfo) = {

    println("Start Time: " + TimeUtils.getTodayDate(TimeConstants.DATE_TIME_FORMAT_MS))
    val yesterdayDateFolder = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val saveMode = params.saveMode
    // var gap = 1
    val hiveContext = Spark.getHiveContext()

    // calculate yesterday date
    // val cal = Calendar.getInstance()
    // cal.add(Calendar.DATE, -gap)
    // val dateFormat = new SimpleDateFormat("dd/MM/YYYY")
    // var dt = dateFormat.format(cal.getTime())

    // val mFormat = new SimpleDateFormat("MM")
    // var year = cal.get(Calendar.YEAR)
    // var day = cal.get(Calendar.DAY_OF_MONTH)
    // var month = mFormat.format(cal.getTime())

    val tablename = "merge.merge_pagevisit"
    val finalTempTable = "finalpagevisit"

    val userDeviceMapPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.CLICKSTREAM, DataSets.USER_DEVICE_MAP_APP, DataSets.DAILY_MODE, yesterdayDateFolder)

    if (DataWriter.canWrite(saveMode, userDeviceMapPath)) {
      var useridDeviceidFrame = getAppIdUserIdData(yesterdayDateFolder, tablename)
      var UserObj = new GroupData()
      UserObj.calculateColumns(useridDeviceidFrame)

      // user device mapping
      var userDeviceMapping = UserDeviceMapping
        .getUserDeviceMapApp(useridDeviceidFrame)
      DataWriter.writeParquet(userDeviceMapping, userDeviceMapPath, saveMode)
    }

    // variable 1
    var surf1VariablePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.CLICKSTREAM, "Surf1ProcessedVariable", DataSets.DAILY_MODE, yesterdayDateFolder)
    if (DataWriter.canWrite(saveMode, surf1VariablePath)) {
      val dMY = TimeUtils.getMonthAndYear(yesterdayDateFolder, TimeConstants.DATE_FORMAT_FOLDER)
      val variableSurf1 = GetSurfVariables.listOfProductsViewedInSession(hiveContext, tablename, dMY.year, dMY.day, dMY.month + 1)
      DataWriter.writeParquet(variableSurf1, surf1VariablePath, saveMode)
    }
  }

  def startSurf3Variable(params: ParamInfo) = {
    val yesterdayDateFolder = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val saveMode = params.saveMode

    // var gap = 1
    val hiveContext = Spark.getHiveContext()

    // calculate yesterday date
    // val cal = Calendar.getInstance()
    // cal.add(Calendar.DATE, -1)
    val tablename = "merge.merge_pagevisit"

    // val dateFormat = new SimpleDateFormat("dd/MM/YYYY")
    // var dt = dateFormat.format(cal.getTime())

    val dayBeforeYesterdayDateFolder = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER, yesterdayDateFolder)

    val currentMergedDataPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.CLICKSTREAM, "Surf3mergedData", DataSets.DAILY_MODE, yesterdayDateFolder)
    var processedVariablePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.CLICKSTREAM, "Surf3ProcessedVariable", DataSets.DAILY_MODE, yesterdayDateFolder)

    if (DataWriter.canWrite(saveMode, currentMergedDataPath) || DataWriter.canWrite(saveMode,processedVariablePath)) {
      var oldMergedDataPath = PathBuilder.buildPath(ConfigConstants.READ_OUTPUT_PATH, DataSets.CLICKSTREAM, "Surf3mergedData", DataSets.DAILY_MODE, dayBeforeYesterdayDateFolder)

      var oldMergedData: DataFrame = null

      // check if merged data exists
      if (DataVerifier.dataExists(oldMergedDataPath)) {
        oldMergedData = hiveContext.read.load(oldMergedDataPath)
      }

      var useridDeviceidFrame = getAppIdUserIdData(yesterdayDateFolder, tablename)
      var UserObj = new GroupData()
      UserObj.calculateColumns(useridDeviceidFrame)
      val userWiseData: RDD[(String, Row)] = UserObj.groupDataByAppUser(hiveContext, useridDeviceidFrame)
      var incremental = GetSurfVariables.Surf3Incremental(userWiseData, UserObj, hiveContext)

      if (null != oldMergedData) {
        var processedVariable = GetSurfVariables.ProcessSurf3Variable(oldMergedData, incremental)
        //processedVariable.write.save(processedVariablePath)
        DataWriter.writeParquet(processedVariable, processedVariablePath, saveMode)
      }
      var mergedData = GetSurfVariables.mergeSurf3Variable(hiveContext, oldMergedData, incremental, yesterdayDateFolder)
      //mergedData.write.parquet(currentMergedDataPath)
      DataWriter.writeParquet(mergedData, currentMergedDataPath, saveMode)
    }

  }

  def getAppIdUserIdData(date: String, tablename: String): DataFrame = {
    val hiveContext = Spark.getHiveContext()
    // val sqlContext = Spark.getSqlContext()

    // val mFormat = new SimpleDateFormat("MM")
    // val year = cal.get(Calendar.YEAR)
    // val day = cal.get(Calendar.DAY_OF_MONTH)
    // val month = mFormat.format(cal.getTime())
    val dMY = TimeUtils.getMonthAndYear(date, TimeConstants.DATE_FORMAT_FOLDER)

    var pagevisit: DataFrame = GetMergedClickstreamData.mergeAppsWeb(hiveContext, tablename, dMY.year, dMY.day, dMY.month + 1)

    //var attributeObj: UserAttribution = new UserAttribution(hiveContext, sqlContext, pagevisit)
    //var userAttributedData: DataFrame = attributeObj.attribute()
    var UserObj = new GroupData()
    //var useridDeviceidFrame = UserObj.appuseridCreation(userAttributedData)
    var useridDeviceidFrame = UserObj.appuseridCreation(pagevisit)

    return useridDeviceidFrame
  }
}
