package oneTimeScripts

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.PageVisitVariables
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.spark.sql.functions._


/**
 * Created by mubarak on 19/11/15.
 */
object GetAd4PushID4mDeviceID {
  def getAd4PushId(deviceIds: DataFrame, ad4push: DataFrame): DataFrame={
    val res = deviceIds.join(ad4push, deviceIds("bid") === ad4push(PageVisitVariables.BROWSER_ID), SQL.LEFT_OUTER)
          .select(deviceIds("bid"),
                  ad4push(PageVisitVariables.ADD4PUSH))
    res
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GetAd4pushId")
    Spark.init(conf)
    val deviceIds= DataReader.getDataFrame4mCsv("/user/mubarak/20151117_sports_app_custs_did.csv", "true", ",")
    val ad4push = DataReader.getDataFrame("/data/test/output", DataSets.EXTRAS, DataSets.AD4PUSH_ID, DataSets.FULL_MERGE_MODE, "2015/11/17")
    val ad4pushIds = getAd4PushId(deviceIds, ad4push)
    ad4pushIds.coalesce(1).write.mode(SaveMode.valueOf(DataSets.IGNORE_SAVEMODE))
      .format("com.databricks.spark.csv").option("header", "true")
      .option("delimiter", ",").save("/user/mubarak/ad4pushIds")

  }

}
