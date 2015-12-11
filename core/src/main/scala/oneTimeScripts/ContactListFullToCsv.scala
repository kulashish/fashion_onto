package oneTimeScripts

import java.io.File

import com.jabong.dap.common.Spark
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.DataVerifier
import com.jabong.dap.data.write.DataWriter
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ DataFrame, SaveMode }

/**
 * Created by mubarak on 10/12/15.
 */
object ContactListFullToCsv {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ContactListFullToCsv")
    Spark.init(conf)

    val incrDate = args(0).trim

    val clm = DataReader.getDataFrame("hdfs://dataplatform-master.jabong.com:8020/data/output", DataSets.VARIABLES, DataSets.CONTACT_LIST_MOBILE, DataSets.FULL_MERGE_MODE, incrDate)

    val writePath = DataWriter.getWritePath("hdfs://dataplatform-master.jabong.com:8020/data/tmp", DataSets.VARIABLES, DataSets.CONTACT_LIST_MOBILE, DataSets.FULL, incrDate)
    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    writeCsv(clm, writePath, fileDate + "_CONTACTS_LIST", DataSets.IGNORE_SAVEMODE, "true", ";", 10)
  }

  def writeCsv(df: DataFrame, writePath: String, csvFileName: String, saveMode: String, header: String, delimeter: String, numParts: Int) {
    if (DataWriter.canWrite(saveMode, writePath)) {
      df.coalesce(numParts).write.mode(SaveMode.valueOf(saveMode)).format("com.databricks.spark.csv")
        .option("header", header).option("delimiter", delimeter).save(writePath)
      println("CSV Data written successfully to the following Path: " + writePath)
      var csvSrcFile, csvdestFile: String = ""
      if (numParts == 1) {
        csvSrcFile = writePath + File.separator + "part-00000"
        csvdestFile = writePath + File.separator + csvFileName + ".csv"
        DataVerifier.rename(csvSrcFile, csvdestFile)
      } else {
        //TODO This will work only till 9 partitions. Will need to fix in case we hit more than 9 partitions.
        for (n <- 0 to numParts - 1) {
          csvSrcFile = writePath + File.separator + "part-0000" + n
          csvdestFile = writePath + File.separator + csvFileName + "_" + n + ".csv"
          DataVerifier.rename(csvSrcFile, csvdestFile)
        }
      }
    }
  }

}
