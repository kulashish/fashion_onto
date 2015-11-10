package oneTimeScripts

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.schema.SchemaUtils
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.customer.schema.CustEmailSchema
import org.apache.spark.SparkConf

/**
 * Created by samathashetty on 9/11/15.
 */
object loadCustEmailResponse {
  def main(args: Array[String]) = {
    Spark.init(new SparkConf().setAppName("loadCustEmailResponse"))

    val date = args(0).trim

    val fullPath = args(1).trim

    val saveDate = args(2).trim

    val inputCsv = DataReader.getDataFrame4mCsv(fullPath, "true", "|")
    val custOutputDf = SchemaUtils.addColumns(inputCsv, CustEmailSchema.effective_Smry_Schema)

    val savePathFull = DataWriter.getWritePath(ConfigConstants.TMP_PATH, DataSets.VARIABLES,
      DataSets.CUST_EMAIL_RESPONSE, DataSets.FULL_MERGE_MODE, saveDate)

    DataWriter.writeParquet(custOutputDf, savePathFull, DataSets.IGNORE_SAVEMODE)


  }
}