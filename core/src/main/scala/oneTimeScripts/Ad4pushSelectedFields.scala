package oneTimeScripts

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.variables.Ad4pushVariables
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

/**
 * Created by jabong on 9/10/15.
 */
object Ad4pushSelectedFields {

  def getSelectedFileds(domain: String, code: String) = {
    val curDate = TimeUtils.yesterday(TimeConstants.DATE_FORMAT)
    val devicesData = DataReader.getDataFrame(DataSets.AD4PUSH, domain, DataSets.FULL_MERGE_MODE, curDate)
    println("Starting for " + domain)
    println(devicesData.count())
    val res = devicesData.select(allZero2Null(Ad4pushVariables.LOGIN_USER_ID),
                                  Ad4pushVariables.LASTOPEN,
                                  Ad4pushVariables.SYSTEM_OPTIN_NOTIFS,
                                  Ad4pushVariables.FEEDBACK
     ).na.drop(Array(Ad4pushVariables.LOGIN_USER_ID)).dropDuplicates()
    val SELECTED = domain + "_selected"
    val csvFileName = "exportDevices_" + code + "_" + curDate
    println("writing file with recs: " + res.count())
    DataWriter.writeCsv(res, DataSets.AD4PUSH, SELECTED, DataSets.FULL_MERGE_MODE, curDate, csvFileName, DataSets.OVERWRITE_SAVEMODE, "true", ";")
  }

   def main(args: Array[String]) {
     val conf = new SparkConf().setAppName("Ad4pushSelectedFields")
     Spark.init(conf)
     getSelectedFileds(DataSets.DEVICES_ANDROID, DataSets.ANDROID_CODE)
     getSelectedFileds(DataSets.DEVICES_IOS, DataSets.IOS_CODE)
   }

  def allZero2Null(str: String): String = {
    if (null != str && (0 < str.length || str.matches("^[0]*"))) {
      return null
    }
    str
  }

  val allZero2Null = udf((str: String) => allZero2Null(str: String))

}
