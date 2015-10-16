package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.CustomerVariables
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.UdfUtils
import com.jabong.dap.common.{ OptionUtils, Spark }
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

import scala.collection.mutable.ArrayBuffer

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

    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    DataWriter.writeCsv(dfInc.na.fill(""), DataSets.VARIABLES, DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART2, DataSets.DAILY_MODE, incrDate, "53699_70806_" + fileDate + "_Customer_PREFERRED_TIMESLOT_part2", DataSets.IGNORE_SAVEMODE, "true", ";")

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
      val joinDF = MergeUtils.joinOldAndNewDF(dfInc, dfFullCPOTPart2, CustomerVariables.CUSTOMER_ID)

      val dfFull = joinDF.select(
        coalesce(joinDF(CustomerVariables.NEW_ + CustomerVariables.CUSTOMER_ID), joinDF(CustomerVariables.CUSTOMER_ID)) as CustomerVariables.CUSTOMER_ID,
        joinDF(CustomerVariables.NEW_ + CustomerVariables.ORDER_0).+(joinDF(CustomerVariables.ORDER_0)) as CustomerVariables.ORDER_0,
        joinDF(CustomerVariables.NEW_ + CustomerVariables.ORDER_1).+(joinDF(CustomerVariables.ORDER_1)) as CustomerVariables.ORDER_1,
        joinDF(CustomerVariables.NEW_ + CustomerVariables.ORDER_2).+(joinDF(CustomerVariables.ORDER_2)) as CustomerVariables.ORDER_2,
        joinDF(CustomerVariables.NEW_ + CustomerVariables.ORDER_3).+(joinDF(CustomerVariables.ORDER_3)) as CustomerVariables.ORDER_3,
        joinDF(CustomerVariables.NEW_ + CustomerVariables.ORDER_4).+(joinDF(CustomerVariables.ORDER_4)) as CustomerVariables.ORDER_4,
        joinDF(CustomerVariables.NEW_ + CustomerVariables.ORDER_5).+(joinDF(CustomerVariables.ORDER_5)) as CustomerVariables.ORDER_5,
        joinDF(CustomerVariables.NEW_ + CustomerVariables.ORDER_6).+(joinDF(CustomerVariables.ORDER_6)) as CustomerVariables.ORDER_6,
        joinDF(CustomerVariables.NEW_ + CustomerVariables.ORDER_7).+(joinDF(CustomerVariables.ORDER_7)) as CustomerVariables.ORDER_7,
        joinDF(CustomerVariables.NEW_ + CustomerVariables.ORDER_8).+(joinDF(CustomerVariables.ORDER_8)) as CustomerVariables.ORDER_8,
        joinDF(CustomerVariables.NEW_ + CustomerVariables.ORDER_9).+(joinDF(CustomerVariables.ORDER_9)) as CustomerVariables.ORDER_9,
        joinDF(CustomerVariables.NEW_ + CustomerVariables.ORDER_10).+(joinDF(CustomerVariables.ORDER_10)) as CustomerVariables.ORDER_10,
        joinDF(CustomerVariables.NEW_ + CustomerVariables.ORDER_11).+(joinDF(CustomerVariables.ORDER_11)) as CustomerVariables.ORDER_11)

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
        UdfUtils.getMaxSlotValue(
          ArrayBuffer(
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

      (dfFullFinal.except(dfFullCPOTPart2), dfFullFinal)
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
