package com.jabong.dap.model.responsys.campaigndata

import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.CustomerVariables
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.UdfUtils
import com.jabong.dap.common.{ OptionUtils, Spark }
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.customer.schema.CustVarSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ DataFrame, Row }

/**
 * Created by raghu on 13/10/15.
 */
object CustomerPreferredTimeslotPart1 {

  def start(params: ParamInfo) = {

    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))

    val (dfOpen, dfClick) = readDF(incrDate)

    val (dfCPTPart1) = getCPOTPart1(dfOpen, dfClick)

    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)

    DataWriter.writeCsv(dfCPTPart1.na.fill(""), DataSets.VARIABLES, DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART1, DataSets.DAILY_MODE, incrDate, "53699_70792_" + fileDate + "_Customer_PREFERRED_TIMESLOT_part1", DataSets.IGNORE_SAVEMODE, "true", ";")

  }

  /**
   *
   * @param dfOpen
   * @param dfClick
   * @return
   */
  def getCPOTPart1(dfOpen: DataFrame, dfClick: DataFrame): (DataFrame) = {

    val dfOpenCPOT = getCPOT(dfOpen, CustVarSchema.emailOpen)
    val dfClickCPOT = getCPOT(dfClick, CustVarSchema.emailClick).withColumnRenamed(CustomerVariables.CUSTOMER_ID, "click_" + CustomerVariables.CUSTOMER_ID)

    val dfCPOTPart1 = dfOpenCPOT.join(dfClickCPOT, dfOpenCPOT(CustomerVariables.CUSTOMER_ID) === dfClickCPOT("click_" + CustomerVariables.CUSTOMER_ID))
      .select(
        coalesce(dfOpenCPOT(CustomerVariables.CUSTOMER_ID), dfClickCPOT("click_" + CustomerVariables.CUSTOMER_ID)) as CustomerVariables.CUSTOMER_ID,
        dfOpenCPOT(CustomerVariables.OPEN_0),
        dfOpenCPOT(CustomerVariables.OPEN_1),
        dfOpenCPOT(CustomerVariables.OPEN_2),
        dfOpenCPOT(CustomerVariables.OPEN_3),
        dfOpenCPOT(CustomerVariables.OPEN_4),
        dfOpenCPOT(CustomerVariables.OPEN_5),
        dfOpenCPOT(CustomerVariables.OPEN_6),
        dfOpenCPOT(CustomerVariables.OPEN_7),
        dfOpenCPOT(CustomerVariables.OPEN_8),
        dfOpenCPOT(CustomerVariables.OPEN_9),
        dfOpenCPOT(CustomerVariables.OPEN_10),
        dfOpenCPOT(CustomerVariables.OPEN_11),
        dfClickCPOT(CustomerVariables.CLICK_0),
        dfClickCPOT(CustomerVariables.CLICK_1),
        dfClickCPOT(CustomerVariables.CLICK_2),
        dfClickCPOT(CustomerVariables.CLICK_3),
        dfClickCPOT(CustomerVariables.CLICK_4),
        dfClickCPOT(CustomerVariables.CLICK_5),
        dfClickCPOT(CustomerVariables.CLICK_6),
        dfClickCPOT(CustomerVariables.CLICK_7),
        dfClickCPOT(CustomerVariables.CLICK_8),
        dfClickCPOT(CustomerVariables.CLICK_9),
        dfClickCPOT(CustomerVariables.CLICK_10),
        dfClickCPOT(CustomerVariables.CLICK_11),
        dfOpenCPOT(CustomerVariables.PREFERRED_OPEN_TIMESLOT),
        dfClickCPOT(CustomerVariables.PREFERRED_CLICK_TIMESLOT)
      )

    dfCPOTPart1.na.fill(0)
  }

  /**
   *
   * @param dfSalesOrder
   * @param schema
   * @return
   */
  def getCPOT(dfSalesOrder: DataFrame, schema: StructType): DataFrame = {

    val salesOrder = dfSalesOrder.select("CUSTOMER_ID", CustomerVariables.EVENT_CAPTURED_DT)
      .sort("CUSTOMER_ID", CustomerVariables.EVENT_CAPTURED_DT)

    val soMapReduce = salesOrder.map(r => ((r(0), TimeUtils.timeToSlot(r(1).toString, TimeConstants.DATE_TIME_FORMAT)), 1)).reduceByKey(_ + _)

    val soNewMap = soMapReduce.map{ case (key, value) => (key._1, (key._2.asInstanceOf[Int], value.toInt)) }

    val soGrouped = soNewMap.groupByKey().map{ case (key, value) => (key.toString, UdfUtils.getCompleteSlotData(value)) }

    val rowRDD = soGrouped.map({
      case (key, value) =>
        Row(
          key,
          value._1,
          value._2,
          value._3,
          value._4,
          value._5,
          value._6,
          value._7,
          value._8,
          value._9,
          value._10,
          value._11,
          value._12,
          value._13)
    })

    // Apply the schema to the RDD.
    val df = Spark.getSqlContext().createDataFrame(rowRDD, schema)

    df
  }

  /**
   *
   * @param incrDate
   * @return
   */
  def readDF(incrDate: String): (DataFrame, DataFrame) = {

    val fileDate = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)

    val dfOpen = DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, DataSets.OPEN, DataSets.DAILY_MODE, incrDate, "53699_" + "OPEN_" + fileDate + ".txt", "true", ";")

    val dfClick = DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, DataSets.CLICK, DataSets.DAILY_MODE, incrDate, "53699_" + "CLICK_" + fileDate + ".txt", "true", ";")

    (dfOpen, dfClick)
  }

}
