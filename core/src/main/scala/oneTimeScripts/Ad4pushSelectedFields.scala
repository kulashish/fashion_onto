package oneTimeScripts

import java.io.File

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.variables.Ad4pushVariables
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.DataVerifier
import com.jabong.dap.data.write.DataWriter
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.spark.sql.functions._

/**
 * Created by jabong on 9/10/15.
 */
object Ad4pushSelectedFields {

  def getSelectedFileds(domain: String, code: String) = {
    val curDate = TimeUtils.yesterday(TimeConstants.DATE_FORMAT)
    val devicesData = DataReader.getDataFrame("hdfs://dataplatform-master.jabong.com:8020/data/output", DataSets.AD4PUSH, domain, DataSets.FULL_MERGE_MODE, curDate)
    println("Starting for " + domain)
    println(devicesData.count())
    val res1 = devicesData.select(
      allZero2NullUdf(col(Ad4pushVariables.LOGIN_USER_ID)) as Ad4pushVariables.LOGIN_USER_ID,
      col(Ad4pushVariables.LASTOPEN),
      col(Ad4pushVariables.SYSTEM_OPTIN_NOTIFS),
      col(Ad4pushVariables.FEEDBACK)).dropDuplicates()
    println ("before dropping the null loginUserId " + res1.count())
    val res = res1.na.drop(Array(Ad4pushVariables.LOGIN_USER_ID))
    val SELECTED = domain + "_selected"
    val csvFileName = "exportDevices_" + code + "_" + curDate
    println("writing file with recs: " + res.count())
    writeCsv(res, DataSets.AD4PUSH, SELECTED, DataSets.FULL_MERGE_MODE, curDate, csvFileName, DataSets.OVERWRITE_SAVEMODE, "true", ";")
  }

   def main(args: Array[String]) {
     val conf = new SparkConf().setAppName("Ad4pushSelectedFields")
     Spark.init(conf)
     getSelectedFileds(DataSets.DEVICES_ANDROID, DataSets.ANDROID_CODE)
     getSelectedFileds(DataSets.DEVICES_IOS, DataSets.IOS_CODE)
   }

  def allZero2Null(str: String): String = {
    val nullStr: String = null
    if (null != str && (0 < str.length || str.matches("^[0]*"))) {
      nullStr
    } else {
      str
    }
  }

  val allZero2NullUdf = udf((str: String) => allZero2Null(str: String))

  def writeCsv(df: DataFrame, source: String, tableName: String, mode: String, date: String, csvFileName: String, saveMode: String, header: String, delimeter: String) {
    val writePath = DataWriter.getWritePath("hdfs://dataplatform-master.jabong.com:8020/data/tmp", source, tableName, mode, date)
    if (DataWriter.canWrite(saveMode, writePath)) {
      df.coalesce(1).write.mode(SaveMode.valueOf(saveMode)).format("com.databricks.spark.csv").option("header", header).option("delimiter", delimeter).save(writePath)
      println("CSV Data written successfully to the following Path: " + writePath)
      val csvSrcFile = writePath + File.separator + "part-00000"
      val csvdestFile = writePath + File.separator + csvFileName + ".csv"
      DataVerifier.rename(csvSrcFile, csvdestFile)
    }
  }
}
