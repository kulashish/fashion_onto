package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ ContactListMobileVars, CustomerVariables, SalesOrderVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.UdfUtils
import com.jabong.dap.common.{ OptionUtils, Spark }
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.customer.schema.CustVarSchema
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
    val prevDate = OptionUtils.getOptValue(params.fullDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER, incrDate))

    val (dfIncSalesOrder, dfFullCPOTPart2, dfCmr) = readDF(paths, incrDate, prevDate)

    val (dfInc, dfFullFinal) = getCPOTPart2(dfIncSalesOrder, dfFullCPOTPart2, dfCmr)

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
  def getCPOTPart2(dfIncSalesOrder: DataFrame, dfFullCPOTPart2: DataFrame, dfCmrFull: DataFrame): (DataFrame, DataFrame) = {

    val dfCPOT = UdfUtils.getCPOT(dfIncSalesOrder.select(col(SalesOrderVariables.FK_CUSTOMER) as "ID", col(SalesOrderVariables.CREATED_AT) as "DATE"), CustVarSchema.customersPreferredOrderTimeslotPart2, TimeConstants.DATE_TIME_FORMAT)

    val dfCmr = dfCmrFull.select(
      dfCmrFull(ContactListMobileVars.UID),
      dfCmrFull(CustomerVariables.ID_CUSTOMER).cast("string") as (CustomerVariables.ID_CUSTOMER)
    )

    val dfInc = dfCPOT.join(dfCmr, dfCmr(CustomerVariables.ID_CUSTOMER) === dfCPOT(CustomerVariables.CUSTOMER_ID), SQL.LEFT_OUTER)
      .select(
        col(ContactListMobileVars.UID) as CustomerVariables.CUSTOMER_ID,
        col(CustomerVariables.ORDER_0),
        col(CustomerVariables.ORDER_1),
        col(CustomerVariables.ORDER_2),
        col(CustomerVariables.ORDER_3),
        col(CustomerVariables.ORDER_4),
        col(CustomerVariables.ORDER_5),
        col(CustomerVariables.ORDER_6),
        col(CustomerVariables.ORDER_7),
        col(CustomerVariables.ORDER_8),
        col(CustomerVariables.ORDER_9),
        col(CustomerVariables.ORDER_10),
        col(CustomerVariables.ORDER_11)
      )

    if (dfFullCPOTPart2 != null) {
      val dfIncrVarBC = Spark.getContext().broadcast(dfInc).value

      //join old and new data frame
      val joinDF = dfFullCPOTPart2.join(dfIncrVarBC, dfFullCPOTPart2(CustomerVariables.CUSTOMER_ID) === dfIncrVarBC(CustomerVariables.CUSTOMER_ID), SQL.FULL_OUTER)
      // MergeUtils.joinOldAndNewDF(dfInc, dfFullCPOTPart2, CustomerVariables.CUSTOMER_ID)

      val dfFull = joinDF.select(
        coalesce(dfIncrVarBC(CustomerVariables.CUSTOMER_ID), dfFullCPOTPart2(CustomerVariables.CUSTOMER_ID)) as CustomerVariables.CUSTOMER_ID,
        dfIncrVarBC(CustomerVariables.ORDER_0).+(dfFullCPOTPart2(CustomerVariables.ORDER_0)) as CustomerVariables.ORDER_0,
        dfIncrVarBC(CustomerVariables.ORDER_1).+(dfFullCPOTPart2(CustomerVariables.ORDER_1)) as CustomerVariables.ORDER_1,
        dfIncrVarBC(CustomerVariables.ORDER_2).+(dfFullCPOTPart2(CustomerVariables.ORDER_2)) as CustomerVariables.ORDER_2,
        dfIncrVarBC(CustomerVariables.ORDER_3).+(dfFullCPOTPart2(CustomerVariables.ORDER_3)) as CustomerVariables.ORDER_3,
        dfIncrVarBC(CustomerVariables.ORDER_4).+(dfFullCPOTPart2(CustomerVariables.ORDER_4)) as CustomerVariables.ORDER_4,
        dfIncrVarBC(CustomerVariables.ORDER_5).+(dfFullCPOTPart2(CustomerVariables.ORDER_5)) as CustomerVariables.ORDER_5,
        dfIncrVarBC(CustomerVariables.ORDER_6).+(dfFullCPOTPart2(CustomerVariables.ORDER_6)) as CustomerVariables.ORDER_6,
        dfIncrVarBC(CustomerVariables.ORDER_7).+(dfFullCPOTPart2(CustomerVariables.ORDER_7)) as CustomerVariables.ORDER_7,
        dfIncrVarBC(CustomerVariables.ORDER_8).+(dfFullCPOTPart2(CustomerVariables.ORDER_8)) as CustomerVariables.ORDER_8,
        dfIncrVarBC(CustomerVariables.ORDER_9).+(dfFullCPOTPart2(CustomerVariables.ORDER_9)) as CustomerVariables.ORDER_9,
        dfIncrVarBC(CustomerVariables.ORDER_10).+(dfFullCPOTPart2(CustomerVariables.ORDER_10)) as CustomerVariables.ORDER_10,
        dfIncrVarBC(CustomerVariables.ORDER_11).+(dfFullCPOTPart2(CustomerVariables.ORDER_11)) as CustomerVariables.ORDER_11)

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
  def readDF(paths: String, incrDate: String, prevDate: String): (DataFrame, DataFrame, DataFrame) = {

    val dfCmr = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, incrDate)

    if (paths != null) {

      val dfIncSalesOrder = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.FULL_MERGE_MODE, incrDate)

      (dfIncSalesOrder, null, dfCmr)
    } else {

      val dfIncSalesOrder = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.DAILY_MODE, incrDate)
      val dfFullCPOTPart2 = DataReader.getDataFrame(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART2, DataSets.FULL_MERGE_MODE, prevDate)

      (dfIncSalesOrder, dfFullCPOTPart2, dfCmr)
    }
  }

}
