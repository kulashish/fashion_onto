package oneTimeScripts

import com.jabong.dap.common.constants.variables.{ SalesOrderItemVariables, SalesOrderVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.{ Spark, Utils }
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.order.variables.SalesOrderItem
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by mubarak on 21/10/15.
 */
object SalesOrderHistoric {

  def processHistoricData() = {
    var i = 0
    val WRITE_OUTPUT_PATH = "hdfs://dataplatform-master.jabong.com:8020/data/test/output"
    val INPUT_PATH = "hdfs://dataplatform-master.jabong.com:8020/data/input"

    for (i <- 91 to 1 by -1) {
      val date = TimeUtils.getDateAfterNDays(-i, TimeConstants.DATE_FORMAT_FOLDER)
      val incrDate = TimeUtils.getDateAfterNDays(-i, TimeConstants.DATE_FORMAT_FOLDER)
      val prevFull = DataReader.getDataFrameOrNull(WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.FULL_MERGE_MODE, date)
      val before7 = TimeUtils.getDateAfterNDays(-7, incrDate)
      val salesRevenue7 = DataReader.getDataFrameOrNull(WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.DAILY_MODE, before7)

      val before30 = TimeUtils.getDateAfterNDays(-30, incrDate)
      val salesRevenue30 = DataReader.getDataFrameOrNull(WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.DAILY_MODE, before30)

      val before90 = TimeUtils.getDateAfterNDays(-90, incrDate)
      val salesRevenue90 = DataReader.getDataFrameOrNull(WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.DAILY_MODE, before90)
      var salesOrderincr: DataFrame = null
      var salesOrderItemincr: DataFrame = null
      if (null == prevFull) {
        salesOrderincr = DataReader.getDataFrameOrNull(INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.FULL_MERGE_MODE, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
          .filter(col(SalesOrderVariables.CREATED_AT) <= TimeUtils.getEndTimestampMS(TimeUtils.getTimeStamp(incrDate, TimeConstants.DATE_FORMAT_FOLDER)))

        salesOrderItemincr = DataReader.getDataFrameOrNull(INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ITEM, DataSets.FULL_MERGE_MODE, incrDate)
      } else {
        val salesOrder = DataReader.getDataFrameOrNull(INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.DAILY_MODE, incrDate)
        val salesOrderItem = DataReader.getDataFrameOrNull(INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ITEM, DataSets.DAILY_MODE, incrDate)
        salesOrderincr = Utils.getOneDayData(salesOrder, SalesOrderVariables.CREATED_AT, incrDate, TimeConstants.DATE_FORMAT_FOLDER)
        salesOrderItemincr = Utils.getOneDayData(salesOrderItem, SalesOrderVariables.CREATED_AT, incrDate, TimeConstants.DATE_FORMAT_FOLDER)
      }
      val salesOrderNew = salesOrderincr.na.fill(Map(
        SalesOrderVariables.GW_AMOUNT -> 0.0
      ))
      salesOrderNew.printSchema()
      salesOrderItemincr.printSchema()
      val saleOrderJoined = salesOrderNew.join(salesOrderItemincr, salesOrderNew(SalesOrderVariables.ID_SALES_ORDER) === salesOrderItemincr(SalesOrderVariables.FK_SALES_ORDER))
        .drop(salesOrderItemincr(SalesOrderItemVariables.CREATED_AT))
      println("count joined: " + saleOrderJoined.count())

      val (salesRevenueVarIncr, salesRevenueVarFull) = SalesOrderItem.getRevenueOrdersCount(saleOrderJoined, prevFull, salesRevenue7, salesRevenue30, salesRevenue90)
      var savePath = DataWriter.getWritePath(WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.FULL_MERGE_MODE, incrDate)
      var savePathDaily = DataWriter.getWritePath(WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.DAILY_MODE, incrDate)
      DataWriter.writeParquet(salesRevenueVarIncr, savePathDaily, DataSets.IGNORE_SAVEMODE)
      DataWriter.writeParquet(salesRevenueVarFull, savePath, DataSets.IGNORE_SAVEMODE)

    }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkExamples")
    Spark.init(conf)
    processHistoricData()

  }

}
