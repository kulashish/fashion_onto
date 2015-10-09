package oneTimeScripts

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.variables.Ad4pushVariables
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by jabong on 9/10/15.
 */
object Ad4pushSelectedFields {

  def getSelectedFileds(readPath: String, savePath: String) ={
    val devicesData= DataReader.getDataFrame4mCsv(readPath, "true", ";")
    val res = devicesData.select(Ad4pushVariables.LOGIN_USER_ID,
                                  Ad4pushVariables.LASTOPEN,
                                  Ad4pushVariables.SYSTEM_OPTIN_NOTIFS,
                                  Ad4pushVariables.FEEDBACK
     ).na.drop(Array(Ad4pushVariables.LOGIN_USER_ID)).dropDuplicates()
    val curDate = TimeUtils.getTodayDate(TimeConstants.DATE_FORMAT_FOLDER)
    DataWriter.writeCsv(res, DataSets.AD4PUSH, DataSets.DEVICES_ANDROID, DataSets.FULL_MERGE_MODE, curDate, savePath, DataSets.FULL_MERGE_MODE, "true", ";")
  }

   def main(args: Array[String]) {
     val conf = new SparkConf().setAppName("SparkExamples")
     Spark.init(conf)
     getSelectedFileds(args(1), args(2))
     }

}
