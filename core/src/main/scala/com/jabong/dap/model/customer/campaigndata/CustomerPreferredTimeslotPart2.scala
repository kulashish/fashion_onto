package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.{ Utils, Spark }
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ SalesOrderVariables, ContactListMobileVars, CustomerVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.{ Udf, UdfUtils }
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.dataFeeds.DataFeedsModel
import com.jabong.dap.model.customer.schema.CustVarSchema
import grizzled.slf4j.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row }

import scala.collection.mutable.{ ArrayBuffer, HashMap }

/**
 * Created by raghu on 13/10/15.
 */
object CustomerPreferredTimeslotPart2 extends DataFeedsModel with Logging {

  def canProcess(incrDate: String, saveMode: String): Boolean = {
    val pathCustomerPreferredTimeslotPart2Full = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART2, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.canWrite(saveMode, pathCustomerPreferredTimeslotPart2Full)
  }

  /**
   *
   * @param paths
   * @param incrDate
   * @param prevDate
   * @return
   */
  def readDF(paths: String, incrDate: String, prevDate: String): HashMap[String, DataFrame] = {

    val dfMap: HashMap[String, DataFrame] = new HashMap[String, DataFrame]()

    val dfCmr = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, incrDate)
    var dfSalesOrderIncr: DataFrame = null
    var dfCPOTPart2PrevFull: DataFrame = null

    dfMap.put("cmrFull", dfCmr)

    if (paths != null) {
      dfSalesOrderIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.FULL_MERGE_MODE, incrDate)
    } else {
      dfSalesOrderIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.DAILY_MODE, incrDate)
      dfCPOTPart2PrevFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART2, DataSets.FULL_MERGE_MODE, prevDate)
    }
    dfMap.put("salesOrderIncr", dfSalesOrderIncr)
    dfMap.put("CPOTPart2PrevFull", dfCPOTPart2PrevFull)
    dfMap
  }

  def process(dfMap: HashMap[String, DataFrame]): HashMap[String, DataFrame] = {
    val dfCmrFull = dfMap("cmrFull")
    val dfSalesOrderIncr = dfMap("salesOrderIncr")
    val dfCPOTPart2PrevFull = dfMap.getOrElse("CPOTPart2PrevFull", null)

    val dfCPOT = Utils.getCPOT(dfSalesOrderIncr.select(SalesOrderVariables.FK_CUSTOMER, SalesOrderVariables.CREATED_AT), CustVarSchema.customersPreferredOrderTimeslotPart2, TimeConstants.DATE_TIME_FORMAT)

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
        col(CustomerVariables.ORDER_11),
        col(CustomerVariables.PREFERRED_ORDER_TIMESLOT)
      )

    val dfWrite: HashMap[String, DataFrame] = new HashMap[String, DataFrame]()

    if (dfCPOTPart2PrevFull != null) {
      val dfIncrVarBC = Spark.getContext().broadcast(dfInc).value

      //join old and new data frame
      val joinDF = dfCPOTPart2PrevFull.join(dfIncrVarBC, dfCPOTPart2PrevFull(CustomerVariables.CUSTOMER_ID) === dfIncrVarBC(CustomerVariables.CUSTOMER_ID), SQL.FULL_OUTER)

      val dfFull = joinDF.select(
        coalesce(dfIncrVarBC(CustomerVariables.CUSTOMER_ID), dfCPOTPart2PrevFull(CustomerVariables.CUSTOMER_ID)) as CustomerVariables.CUSTOMER_ID,
        Udf.addInt(dfIncrVarBC(CustomerVariables.ORDER_0), dfCPOTPart2PrevFull(CustomerVariables.ORDER_0)) as CustomerVariables.ORDER_0,
        Udf.addInt(dfIncrVarBC(CustomerVariables.ORDER_1), dfCPOTPart2PrevFull(CustomerVariables.ORDER_1)) as CustomerVariables.ORDER_1,
        Udf.addInt(dfIncrVarBC(CustomerVariables.ORDER_2), dfCPOTPart2PrevFull(CustomerVariables.ORDER_2)) as CustomerVariables.ORDER_2,
        Udf.addInt(dfIncrVarBC(CustomerVariables.ORDER_3), dfCPOTPart2PrevFull(CustomerVariables.ORDER_3)) as CustomerVariables.ORDER_3,
        Udf.addInt(dfIncrVarBC(CustomerVariables.ORDER_4), dfCPOTPart2PrevFull(CustomerVariables.ORDER_4)) as CustomerVariables.ORDER_4,
        Udf.addInt(dfIncrVarBC(CustomerVariables.ORDER_5), dfCPOTPart2PrevFull(CustomerVariables.ORDER_5)) as CustomerVariables.ORDER_5,
        Udf.addInt(dfIncrVarBC(CustomerVariables.ORDER_6), dfCPOTPart2PrevFull(CustomerVariables.ORDER_6)) as CustomerVariables.ORDER_6,
        Udf.addInt(dfIncrVarBC(CustomerVariables.ORDER_7), dfCPOTPart2PrevFull(CustomerVariables.ORDER_7)) as CustomerVariables.ORDER_7,
        Udf.addInt(dfIncrVarBC(CustomerVariables.ORDER_8), dfCPOTPart2PrevFull(CustomerVariables.ORDER_8)) as CustomerVariables.ORDER_8,
        Udf.addInt(dfIncrVarBC(CustomerVariables.ORDER_9), dfCPOTPart2PrevFull(CustomerVariables.ORDER_9)) as CustomerVariables.ORDER_9,
        Udf.addInt(dfIncrVarBC(CustomerVariables.ORDER_10), dfCPOTPart2PrevFull(CustomerVariables.ORDER_10)) as CustomerVariables.ORDER_10,
        Udf.addInt(dfIncrVarBC(CustomerVariables.ORDER_11), dfCPOTPart2PrevFull(CustomerVariables.ORDER_11)) as CustomerVariables.ORDER_11)

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

      dfWrite.put("CPOTPart2Full", dfFullFinal)
      dfWrite.put("CPOTPart2Incr", dfFullFinal.except(dfCPOTPart2PrevFull))
    } else {
      dfWrite.put("CPOTPart2Full", dfInc)
      dfWrite.put("CPOTPart2Incr", dfInc)
    }
    dfWrite
  }

  def write(dfWriteMap: HashMap[String, DataFrame], saveMode: String, incrDate: String) = {
    val pathCustomerPreferredTimeslotPart2Full = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART2, DataSets.FULL_MERGE_MODE, incrDate)
    if (DataWriter.canWrite(saveMode, pathCustomerPreferredTimeslotPart2Full)) {
      DataWriter.writeParquet(dfWriteMap("CPOTPart2Full"), pathCustomerPreferredTimeslotPart2Full, saveMode)
    }
    val pathCustomerPreferredTimeslotPart2Incr = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART2, DataSets.DAILY_MODE, incrDate)
    if (DataWriter.canWrite(saveMode, pathCustomerPreferredTimeslotPart2Incr)) {
      DataWriter.writeParquet(dfWriteMap("CPOTPart2Incr"), pathCustomerPreferredTimeslotPart2Incr, saveMode)
    }
    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    DataWriter.writeCsv(dfWriteMap("CPOTPart2Incr").na.fill(""), DataSets.VARIABLES, DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART2, DataSets.DAILY_MODE, incrDate, fileDate + "_Customer_PREFERRED_TIMESLOT_part2", DataSets.IGNORE_SAVEMODE, "true", ";", 1)
  }
}
