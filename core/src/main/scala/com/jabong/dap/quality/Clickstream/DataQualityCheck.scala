package com.jabong.dap.quality.Clickstream

/**
 * Created by tejas on 13/8/15.
 */

object DataQualityCheck extends java.io.Serializable {

  def main(args: Array[String]): Unit = {


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