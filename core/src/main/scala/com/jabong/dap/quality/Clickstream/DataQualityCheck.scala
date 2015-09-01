package com.jabong.dap.quality.Clickstream


import java.text.SimpleDateFormat
import java.util.Calendar

import com.jabong.dap.common.mail.ScalaMail

//import _root_.jabong.ScalaMail
import com.jabong.dap.common.Spark
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf


/**
 * Created by tejas on 13/8/15.
 */

object DataQualityCheck extends java.io.Serializable {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Clickstream Surf Variables").set("spark.driver.allowMultipleContexts", "true")
    Spark.init(conf)
    val hiveContext = Spark.getHiveContext()
    val sqlContext = Spark.getSqlContext()
    var gap = args(0).toInt
    var tablename = args(1)
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -gap)
    val mFormat = new SimpleDateFormat("MM")
    var year = cal.get(Calendar.YEAR).toString
    var day = cal.get(Calendar.DAY_OF_MONTH).toString
    var month = mFormat.format(cal.getTime())
    val dateFormat = new SimpleDateFormat("dd/MM/YYYY")
    var dt = dateFormat.format(cal.getTime())
    //var res1 = DataQualityMethods.Artemisdaily(hiveContext, day, month, year, tablename)
    val output = DataQualityMethods.Artemisdaily(hiveContext, day: String, month: String, year: String, tablename: String)
    write("hdfs://172.16.84.37:8020", "./Automation1/",output.getBytes())

    ScalaMail.sendMessage("tejas.jain@jabong.com","","","tejas.jain@jabong.com",output,"Quality Report","")

    //println("ScalaMailMain")
  }


  def write(uri: String, filePath: String,data: Array[Byte]) = {
    System.setProperty("HADOOP_USER_NAME", "tjain")
    val path = new Path(filePath)
    val conf = new Configuration()
    conf.set("fs.defaultFS", uri)
    val fs = FileSystem.get(conf)
    val os = fs.create(path)
    os.write(data)
    fs.close()
  }
}
/*
    var gap = args(3).toInt
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

    var useridDeviceidFrame = getAppIdUserIdData(cal, tablename)
    var UserObj = new GroupData()
    UserObj.calculateColumns(useridDeviceidFrame)
    val userWiseData: RDD[(String, Row)] = UserObj.groupDataByAppUser(hiveContext, useridDeviceidFrame)

    cal.add(Calendar.DATE, -1)
    year = cal.get(Calendar.YEAR)
    day = cal.get(Calendar.DAY_OF_MONTH)
    month = mFormat.format(cal.getTime())
    var oldMergedDataPath = args(1) + "/" + year + "/" + month + "/" + day + "/Surf3mergedData"
    var oldMergedData: DataFrame = null
    if (DataVerifier.dataExists(oldMergedDataPath)) {
      oldMergedData = sqlContext.read.load(oldMergedDataPath)
    }
    val today = "_daily"
    var incremental = GetSurfVariables.Surf3Incremental(userWiseData, UserObj, hiveContext)
    var processedVariable = GetSurfVariables.ProcessSurf3Variable(oldMergedData, incremental)
    var mergedData = GetSurfVariables.mergeSurf3Variable(hiveContext, oldMergedData, incremental, dt)
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

  */
