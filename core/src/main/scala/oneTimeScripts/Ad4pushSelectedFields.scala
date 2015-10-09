package oneTimeScripts

import java.io.File

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.variables.Ad4pushVariables
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.common.udf.Udf
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
    val res = devicesData.select(
      Udf.allZero2NullUdf(col(Ad4pushVariables.LOGIN_USER_ID)) as Ad4pushVariables.LOGIN_USER_ID,
      col(Ad4pushVariables.LASTOPEN),
      col(Ad4pushVariables.SYSTEM_OPTIN_NOTIFS),
      col(Ad4pushVariables.FEEDBACK)
    ).na.drop(Array(Ad4pushVariables.LOGIN_USER_ID)).dropDuplicates()

    val SELECTED = domain + "_selected"
    val csvFileName = "exportDevices_" + code + "_" + curDate
    println("writing file with recs: " + res.count())
    writeCsv(res, DataSets.AD4PUSH, SELECTED, DataSets.FULL_MERGE_MODE, curDate, csvFileName, "true", ";")
  }

   def main(args: Array[String]) {
     val conf = new SparkConf().setAppName("Ad4pushSelectedFields")
     Spark.init(conf)
     getSelectedFileds(DataSets.DEVICES_ANDROID, DataSets.ANDROID_CODE)
     getSelectedFileds(DataSets.DEVICES_IOS, DataSets.IOS_CODE)
   }

  def writeCsv(df: DataFrame, source: String, tableName: String, mode: String, date: String, csvFileName: String, header: String, delimeter: String) {
    val writePath = DataWriter.getWritePath("hdfs://dataplatform-master.jabong.com:8020/data/tmp", source, tableName, mode, date)
    df.coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header", header).option("delimiter", delimeter).save(writePath)
    println("CSV Data written successfully to the following Path: " + writePath)
    val csvSrcFile = writePath + File.separator + "part-00000"
    val csvdestFile = writePath + File.separator + csvFileName + ".csv"
    DataVerifier.rename(csvSrcFile, csvdestFile)
  }
}
