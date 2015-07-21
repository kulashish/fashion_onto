package com.jabong.dap.model.clickstream.variables

import java.text.SimpleDateFormat
import java.util.Calendar

import com.jabong.dap.common.Spark
import com.jabong.dap.model.clickstream.utils.{GroupData, GetMergedClickstreamData}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, Row}
import org.apache.spark.{SparkContext, SparkConf}

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
    val currentMergedDataPath = args(1)+"/"+year+"/"+month+"/"+day+"/Surf3mergedData"
    var processedVariablePath = args(2)+"/"+year+"/"+month+"/"+day+"/Surf3ProcessedVariable"
    cal.add(Calendar.DATE, -1);
    year = cal.get(Calendar.YEAR);
    day = cal.get(Calendar.DAY_OF_MONTH);
    month = mFormat.format(cal.getTime());
    var oldMergedDataPath = args(1)+"/"+year+"/"+month+"/"+day+"/Surf3mergedData"
    var sqlContext = Spark.getSqlContext()
    var oldMergedData = hiveContext.parquetFile(oldMergedDataPath)
    val today = "_daily"
    var UserObj = new GroupData(hiveContext, pagevisit)
    var useridDeviceidFrame = UserObj.appuseridCreation()
    UserObj.calculateColumns(useridDeviceidFrame)
    val userWiseData: RDD[(String, Row)] = UserObj.groupDataByAppUser(useridDeviceidFrame)
    var incremental = GetSurfVariables.Surf3Incremental(userWiseData, UserObj, hiveContext)
    var processedVariable = GetSurfVariables.ProcessSurf3Variable(oldMergedData,incremental)
    var mergedData = GetSurfVariables.mergeSurf3Variable(hiveContext,oldMergedData,incremental,dt)
    mergedData.saveAsParquetFile(currentMergedDataPath)
    incremental.save(processedVariablePath)

    }

}

