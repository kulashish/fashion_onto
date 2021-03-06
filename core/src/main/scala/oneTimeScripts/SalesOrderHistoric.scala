package oneTimeScripts

import com.jabong.dap.common.constants.variables.{ SalesOrderItemVariables, SalesOrderVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.{ Spark, Utils }
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.order.variables.SalesItemRevenue
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by mubarak on 21/10/15.
 */
object SalesOrderHistoric {

  def processHistoricData(start: String, end: String) = {
    val WRITE_OUTPUT_PATH = "hdfs://dataplatform-master.jabong.com:8020/data/test/output"
    val INPUT_PATH = "hdfs://dataplatform-master.jabong.com:8020/data/input"

    val startInt = Integer.parseInt(start) // should start with 91
    val endInt = Integer.parseInt(end) // end at 1

    for (i <- startInt to endInt by -1) {
      val date = TimeUtils.getDateAfterNDays(-i - 1, TimeConstants.DATE_FORMAT_FOLDER)
      val incrDate = TimeUtils.getDateAfterNDays(-i, TimeConstants.DATE_FORMAT_FOLDER)
      val prevFull = DataReader.getDataFrameOrNull(WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.FULL_MERGE_MODE, date)
      val before7 = TimeUtils.getDateAfterNDays(-7, TimeConstants.DATE_FORMAT_FOLDER, incrDate)
      val salesRevenue7 = DataReader.getDataFrameOrNull(WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.DAILY_MODE, before7)
      println("before7: ", before7)
      val before30 = TimeUtils.getDateAfterNDays(-30, TimeConstants.DATE_FORMAT_FOLDER, incrDate)
      println("before30: ", before30)
      val salesRevenue30 = DataReader.getDataFrameOrNull(WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.DAILY_MODE, before30)

      val before90 = TimeUtils.getDateAfterNDays(-90, TimeConstants.DATE_FORMAT_FOLDER, incrDate)
      println("before90: ", before90)
      val salesRevenue90 = DataReader.getDataFrameOrNull(WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.DAILY_MODE, before90)
      var salesOrderincr: DataFrame = null
      var salesOrderItemincr: DataFrame = null
      if (null == prevFull) {
        println("Reading Full -->")
        salesOrderincr = DataReader.getDataFrameOrNull(INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.FULL_MERGE_MODE, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
          .filter(col(SalesOrderVariables.CREATED_AT) <= TimeUtils.getEndTimestampMS(TimeUtils.getTimeStamp(incrDate, TimeConstants.DATE_FORMAT_FOLDER)))

        salesOrderItemincr = DataReader.getDataFrameOrNull(INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ITEM, DataSets.FULL_MERGE_MODE, incrDate)
      } else {
        println("Reading Incr -->")
        val salesOrder = DataReader.getDataFrameOrNull(INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.FULL_MERGE_MODE, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
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
      val (salesRevenueVarIncr, salesRevenueVarFull) = SalesItemRevenue.getRevenueOrdersCount(saleOrderJoined, prevFull, salesRevenue7, salesRevenue30, salesRevenue90)
      if (null == prevFull) {
        var fullPath = DataWriter.getWritePath(WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.FULL_MERGE_MODE, incrDate)
        println("Full Count", salesRevenueVarFull.count())
        salesRevenueVarFull.show(10)
        DataWriter.writeParquet(salesRevenueVarFull, fullPath, DataSets.IGNORE_SAVEMODE)
      } else {
        var savePath = DataWriter.getWritePath(WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.FULL_MERGE_MODE, incrDate)
        var savePathDaily = DataWriter.getWritePath(WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_REVENUE, DataSets.DAILY_MODE, incrDate)
        println("Incr Count", salesRevenueVarIncr.count())
        salesRevenueVarIncr.show(10)
        DataWriter.writeParquet(salesRevenueVarIncr, savePathDaily, DataSets.IGNORE_SAVEMODE)
        println("Full Count", salesRevenueVarFull.count())
        salesRevenueVarFull.show(10)
        DataWriter.writeParquet(salesRevenueVarFull, savePath, DataSets.IGNORE_SAVEMODE)
        Spark.getSqlContext().clearCache()
      }
    }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkExamples")
    Spark.init(conf)
    processHistoricData(args(0), args(1))

  }

}
