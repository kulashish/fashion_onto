package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.{ OptionUtils, Spark }
import com.jabong.dap.common.constants.variables.SalesOrderVariables
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.UdfUtils
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.MergeUtils
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.customer.schema.CustVarSchema
import com.jabong.dap.model.order.variables.SalesOrder
import grizzled.slf4j.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row }

/**
 * Created by raghu on 13/10/15.
 */
object CustomerPreferredTimeslotPart2 extends Logging {

  def start(params: ParamInfo) = {

    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val saveMode = params.saveMode
    val paths = OptionUtils.getOptValue(params.path)
    val prevDate = OptionUtils.getOptValue(params.fullDate, TimeUtils.getDateAfterNDays(-2, TimeConstants.DATE_FORMAT_FOLDER))

    val (dfIncSalesOrder, dfFullCPOTPart2) = readDF(paths, incrDate, prevDate)

    val (dfInc, dfFullFinal) = getCPOTPart2(dfIncSalesOrder, dfFullCPOTPart2)

    val pathCustomerPreferredTimeslotPart2Full = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART2, DataSets.FULL_MERGE_MODE, incrDate)
    if (DataWriter.canWrite(saveMode, pathCustomerPreferredTimeslotPart2Full)) {
      DataWriter.writeParquet(dfFullFinal, pathCustomerPreferredTimeslotPart2Full, saveMode)
    }

    val pathCustomerPreferredTimeslotPart2Inc = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART2, DataSets.DAILY_MODE, incrDate)
    if (DataWriter.canWrite(saveMode, pathCustomerPreferredTimeslotPart2Inc)) {
      DataWriter.writeParquet(dfInc, pathCustomerPreferredTimeslotPart2Inc, saveMode)
    }

  }

  /**
   *
   * @param dfIncSalesOrder
   * @param dfFullCPOTPart2
   * @return
   */
  def getCPOTPart2(dfIncSalesOrder: DataFrame, dfFullCPOTPart2: DataFrame): (DataFrame, DataFrame) = {

    val dfInc = SalesOrder.getCPOT(dfIncSalesOrder)

    if (dfFullCPOTPart2 != null) {

      //join old and new data frame
      val joinDF = MergeUtils.joinOldAndNewDF(dfInc, dfFullCPOTPart2, SalesOrderVariables.CUSTOMER_ID)

      val dfFull = joinDF.select(
        coalesce(joinDF(SalesOrderVariables.NEW_ + SalesOrderVariables.CUSTOMER_ID), joinDF(SalesOrderVariables.CUSTOMER_ID)) as SalesOrderVariables.CUSTOMER_ID,
        joinDF(SalesOrderVariables.NEW_ + SalesOrderVariables.ORDER_0).+(joinDF(SalesOrderVariables.ORDER_0)) as SalesOrderVariables.ORDER_0,
        joinDF(SalesOrderVariables.NEW_ + SalesOrderVariables.ORDER_1).+(joinDF(SalesOrderVariables.ORDER_1)) as SalesOrderVariables.ORDER_1,
        joinDF(SalesOrderVariables.NEW_ + SalesOrderVariables.ORDER_2).+(joinDF(SalesOrderVariables.ORDER_2)) as SalesOrderVariables.ORDER_2,
        joinDF(SalesOrderVariables.NEW_ + SalesOrderVariables.ORDER_3).+(joinDF(SalesOrderVariables.ORDER_3)) as SalesOrderVariables.ORDER_3,
        joinDF(SalesOrderVariables.NEW_ + SalesOrderVariables.ORDER_4).+(joinDF(SalesOrderVariables.ORDER_4)) as SalesOrderVariables.ORDER_4,
        joinDF(SalesOrderVariables.NEW_ + SalesOrderVariables.ORDER_5).+(joinDF(SalesOrderVariables.ORDER_5)) as SalesOrderVariables.ORDER_5,
        joinDF(SalesOrderVariables.NEW_ + SalesOrderVariables.ORDER_6).+(joinDF(SalesOrderVariables.ORDER_6)) as SalesOrderVariables.ORDER_6,
        joinDF(SalesOrderVariables.NEW_ + SalesOrderVariables.ORDER_7).+(joinDF(SalesOrderVariables.ORDER_7)) as SalesOrderVariables.ORDER_7,
        joinDF(SalesOrderVariables.NEW_ + SalesOrderVariables.ORDER_8).+(joinDF(SalesOrderVariables.ORDER_8)) as SalesOrderVariables.ORDER_8,
        joinDF(SalesOrderVariables.NEW_ + SalesOrderVariables.ORDER_9).+(joinDF(SalesOrderVariables.ORDER_9)) as SalesOrderVariables.ORDER_9,
        joinDF(SalesOrderVariables.NEW_ + SalesOrderVariables.ORDER_10).+(joinDF(SalesOrderVariables.ORDER_10)) as SalesOrderVariables.ORDER_10,
        joinDF(SalesOrderVariables.NEW_ + SalesOrderVariables.ORDER_11).+(joinDF(SalesOrderVariables.ORDER_11)) as SalesOrderVariables.ORDER_11)

      val rowRDD = dfFull.map(r => (Row(
        r(0),
        r(1),
        r(2),
        r(3),
        r(4),
        r(5),
        r(6),
        r(7),
        r(8),
        r(9),
        r(10),
        r(11),
        r(12),
        UdfUtils.getMaxSlotValue((
          r(1).asInstanceOf[Int],
          r(2).asInstanceOf[Int],
          r(3).asInstanceOf[Int],
          r(4).asInstanceOf[Int],
          r(5).asInstanceOf[Int],
          r(6).asInstanceOf[Int],
          r(7).asInstanceOf[Int],
          r(8).asInstanceOf[Int],
          r(9).asInstanceOf[Int],
          r(10).asInstanceOf[Int],
          r(11).asInstanceOf[Int],
          r(12).asInstanceOf[Int]))))
      )

      // Apply the schema to the RDD.
      val dfFullFinal = Spark.getSqlContext().createDataFrame(rowRDD, CustVarSchema.customersPreferredOrderTimeslotPart2)

      (dfFull.except(dfFullCPOTPart2), dfFullFinal)
    } else {
      (dfInc, dfInc)
    }

  }

  /**
   *
   * @param paths
   * @param incrDate
   * @param prevDate
   * @return
   */
  def readDF(paths: String, incrDate: String, prevDate: String): (DataFrame, DataFrame) = {

    if (paths != null) {

      val dfIncSalesOrder = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.FULL_MERGE_MODE, incrDate)

      (dfIncSalesOrder, null)
    } else {

      val dfIncSalesOrder = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.DAILY_MODE, incrDate)
      val dfFullCPOTPart2 = DataReader.getDataFrame(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART2, DataSets.FULL_MERGE_MODE, prevDate)

      (dfIncSalesOrder, dfFullCPOTPart2)
    }
  }

}
