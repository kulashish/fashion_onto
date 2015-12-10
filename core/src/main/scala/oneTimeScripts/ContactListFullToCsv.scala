package oneTimeScripts

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.variables.PageVisitVariables
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, DataFrame}

/**
 * Created by mubarak on 10/12/15.
 */
class ContactListFullToCsv {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GetAd4pushId")
    Spark.init(conf)
    val fullPath = args(0).trim

    val incrDate = args(1).trim

    val clm = Spark.getSqlContext().read.parquet(fullPath)

    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    DataWriter.writeCsv(clm, DataSets.VARIABLES, DataSets.CONTACT_LIST_MOBILE, DataSets.FULL, incrDate, fileDate + "_CONTACTS_LIST", DataSets.IGNORE_SAVEMODE, "true", ";", 10)

  }

}
