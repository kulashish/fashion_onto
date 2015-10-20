package com.jabong.dap.model.responsys.campaigndata

import com.jabong.dap.common.constants.SQL
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

import scala.collection.mutable.ArrayBuffer

/**
 * Created by raghu on 13/10/15.
 */
object CustomerPreferredTimeslotPart1 {

  def start(params: ParamInfo) = {

    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val saveMode = params.saveMode
    val paths = OptionUtils.getOptValue(params.path)
    val prevDate = OptionUtils.getOptValue(params.fullDate, TimeUtils.getDateAfterNDays(-2, TimeConstants.DATE_FORMAT_FOLDER))

    val (dfOpen, dfClick, dfPrevFullCPOTPart1) = readDF(paths, incrDate, prevDate)

    val (dfInc, dfFull) = getCPOTPart1(dfOpen, dfClick, dfPrevFullCPOTPart1)

    val pathCustomerPreferredTimeslotPart1Full = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART1, DataSets.FULL_MERGE_MODE, incrDate)
    if (DataWriter.canWrite(saveMode, pathCustomerPreferredTimeslotPart1Full)) {
      DataWriter.writeParquet(dfFull, pathCustomerPreferredTimeslotPart1Full, saveMode)
    }

    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    DataWriter.writeCsv(dfInc.na.fill(""), DataSets.VARIABLES, DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART1, DataSets.DAILY_MODE, incrDate, "53699_70792_" + fileDate + "_Customer_PREFERRED_TIMESLOT_part1", DataSets.IGNORE_SAVEMODE, "true", ";")

  }

  /**
   *
   * @param dfOpen
   * @param dfClick
   * @return
   */
  def getCPOTPart1(dfOpen: DataFrame, dfClick: DataFrame, dfPrevFullCPOTPart1: DataFrame): (DataFrame, DataFrame) = {

    val dfIncCPOTPart1 = getIncCPOTPart1(dfOpen, dfClick)

    if (dfPrevFullCPOTPart1 != null) {

      val dfIncrVarBC = Spark.getContext().broadcast(dfIncCPOTPart1).value
      //join old and new data frame
      val joinDF = dfPrevFullCPOTPart1.join(dfIncrVarBC, dfPrevFullCPOTPart1(CustomerVariables.CUSTOMER_ID) === dfIncrVarBC(CustomerVariables.CUSTOMER_ID), SQL.FULL_OUTER)
      //MergeUtils.joinOldAndNewDF(dfIncrVarBC, dfPrevFullCPOTPart1, CustomerVariables.CUSTOMER_ID)

      val dfFull = joinDF.select(
        coalesce(dfIncrVarBC(CustomerVariables.CUSTOMER_ID), dfPrevFullCPOTPart1(CustomerVariables.CUSTOMER_ID)) as CustomerVariables.CUSTOMER_ID,
        dfIncrVarBC(CustomerVariables.OPEN_0) + dfPrevFullCPOTPart1(CustomerVariables.OPEN_0) as CustomerVariables.OPEN_0,
        dfIncrVarBC(CustomerVariables.OPEN_1) + dfPrevFullCPOTPart1(CustomerVariables.OPEN_1) as CustomerVariables.OPEN_1,
        dfIncrVarBC(CustomerVariables.OPEN_2) + dfPrevFullCPOTPart1(CustomerVariables.OPEN_2) as CustomerVariables.OPEN_2,
        dfIncrVarBC(CustomerVariables.OPEN_3) + dfPrevFullCPOTPart1(CustomerVariables.OPEN_3) as CustomerVariables.OPEN_3,
        dfIncrVarBC(CustomerVariables.OPEN_4) + dfPrevFullCPOTPart1(CustomerVariables.OPEN_4) as CustomerVariables.OPEN_4,
        dfIncrVarBC(CustomerVariables.OPEN_5) + dfPrevFullCPOTPart1(CustomerVariables.OPEN_5) as CustomerVariables.OPEN_5,
        dfIncrVarBC(CustomerVariables.OPEN_6) + dfPrevFullCPOTPart1(CustomerVariables.OPEN_6) as CustomerVariables.OPEN_6,
        dfIncrVarBC(CustomerVariables.OPEN_7) + dfPrevFullCPOTPart1(CustomerVariables.OPEN_7) as CustomerVariables.OPEN_7,
        dfIncrVarBC(CustomerVariables.OPEN_8) + dfPrevFullCPOTPart1(CustomerVariables.OPEN_8) as CustomerVariables.OPEN_8,
        dfIncrVarBC(CustomerVariables.OPEN_9) + dfPrevFullCPOTPart1(CustomerVariables.OPEN_9) as CustomerVariables.OPEN_9,
        dfIncrVarBC(CustomerVariables.OPEN_10) + dfPrevFullCPOTPart1(CustomerVariables.OPEN_10) as CustomerVariables.OPEN_10,
        dfIncrVarBC(CustomerVariables.OPEN_11) + dfPrevFullCPOTPart1(CustomerVariables.OPEN_11) as CustomerVariables.OPEN_11,
        dfIncrVarBC(CustomerVariables.CLICK_0) + dfPrevFullCPOTPart1(CustomerVariables.CLICK_0) as CustomerVariables.CLICK_0,
        dfIncrVarBC(CustomerVariables.CLICK_1) + dfPrevFullCPOTPart1(CustomerVariables.CLICK_1) as CustomerVariables.CLICK_1,
        dfIncrVarBC(CustomerVariables.CLICK_2) + dfPrevFullCPOTPart1(CustomerVariables.CLICK_2) as CustomerVariables.CLICK_2,
        dfIncrVarBC(CustomerVariables.CLICK_3) + dfPrevFullCPOTPart1(CustomerVariables.CLICK_3) as CustomerVariables.CLICK_3,
        dfIncrVarBC(CustomerVariables.CLICK_4) + dfPrevFullCPOTPart1(CustomerVariables.CLICK_4) as CustomerVariables.CLICK_4,
        dfIncrVarBC(CustomerVariables.CLICK_5) + dfPrevFullCPOTPart1(CustomerVariables.CLICK_5) as CustomerVariables.CLICK_5,
        dfIncrVarBC(CustomerVariables.CLICK_6) + dfPrevFullCPOTPart1(CustomerVariables.CLICK_6) as CustomerVariables.CLICK_6,
        dfIncrVarBC(CustomerVariables.CLICK_7) + dfPrevFullCPOTPart1(CustomerVariables.CLICK_7) as CustomerVariables.CLICK_7,
        dfIncrVarBC(CustomerVariables.CLICK_8) + dfPrevFullCPOTPart1(CustomerVariables.CLICK_8) as CustomerVariables.CLICK_8,
        dfIncrVarBC(CustomerVariables.CLICK_9) + dfPrevFullCPOTPart1(CustomerVariables.CLICK_9) as CustomerVariables.CLICK_9,
        dfIncrVarBC(CustomerVariables.CLICK_10) + dfPrevFullCPOTPart1(CustomerVariables.CLICK_10) as CustomerVariables.CLICK_10,
        dfIncrVarBC(CustomerVariables.CLICK_11) + dfPrevFullCPOTPart1(CustomerVariables.CLICK_11) as CustomerVariables.CLICK_11,
        dfIncrVarBC(CustomerVariables.PREFERRED_OPEN_TIMESLOT) + dfPrevFullCPOTPart1(CustomerVariables.PREFERRED_OPEN_TIMESLOT) as CustomerVariables.PREFERRED_OPEN_TIMESLOT,
        dfIncrVarBC(CustomerVariables.PREFERRED_CLICK_TIMESLOT) + dfPrevFullCPOTPart1(CustomerVariables.PREFERRED_CLICK_TIMESLOT) as CustomerVariables.PREFERRED_CLICK_TIMESLOT)

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
        r(13),
        r(14),
        r(15),
        r(16),
        r(17),
        r(18),
        r(19),
        r(20),
        r(21),
        r(22),
        r(23),
        r(24),
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
            r(12).asInstanceOf[Int])),
        UdfUtils.getMaxSlotValue(
          ArrayBuffer(
            r(13).asInstanceOf[Int],
            r(14).asInstanceOf[Int],
            r(15).asInstanceOf[Int],
            r(16).asInstanceOf[Int],
            r(17).asInstanceOf[Int],
            r(18).asInstanceOf[Int],
            r(19).asInstanceOf[Int],
            r(20).asInstanceOf[Int],
            r(21).asInstanceOf[Int],
            r(22).asInstanceOf[Int],
            r(23).asInstanceOf[Int],
            r(24).asInstanceOf[Int]))))
      )

      // Apply the schema to the RDD.
      val dfFullFinal = Spark.getSqlContext().createDataFrame(rowRDD, CustVarSchema.customersPreferredOrderTimeslotPart1)

      (dfFullFinal.except(dfPrevFullCPOTPart1), dfFullFinal)
    } else {
      (dfIncCPOTPart1, dfIncCPOTPart1)
    }

  }

  /**
   *
   * @param dfOpen
   * @param dfClick
   * @return
   */
  def getIncCPOTPart1(dfOpen: DataFrame, dfClick: DataFrame): DataFrame = {

    val dfOpenCPOT = getCPOT(dfOpen, CustVarSchema.emailOpen)
    val dfClickCPOT = getCPOT(dfClick, CustVarSchema.emailClick)

    val dfIncCPOTPart1 = dfOpenCPOT.join(dfClickCPOT, dfOpenCPOT(CustomerVariables.CUSTOMER_ID) === dfClickCPOT(CustomerVariables.CUSTOMER_ID))
      .select(
        coalesce(dfOpenCPOT(CustomerVariables.CUSTOMER_ID), dfClickCPOT(CustomerVariables.CUSTOMER_ID)) as CustomerVariables.CUSTOMER_ID,
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
      ).na.fill(0)

    dfIncCPOTPart1
  }

  /**
   *
   * @param dfIn
   * @param schema
   * @return
   */
  def getCPOT(dfIn: DataFrame, schema: StructType): DataFrame = {

    val dfSelect = dfIn.select(col("CUSTOMER_ID"), col(CustomerVariables.EVENT_CAPTURED_DT))
      .sort("CUSTOMER_ID", CustomerVariables.EVENT_CAPTURED_DT)

    val mapReduce = dfSelect.map(r => ((r(0), TimeUtils.timeToSlot(r(1).toString, TimeConstants.DD_MMM_YYYY_HH_MM_SS)), 1)).reduceByKey(_ + _)

    val newMap = mapReduce.map{ case (key, value) => (key._1, (key._2.asInstanceOf[Int], value.toInt)) }

    val grouped = newMap.groupByKey().map{ case (key, value) => (key.toString, UdfUtils.getCompleteSlotData(value)) }

    val rowRDD = grouped.map({
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
   * @param paths
   * @param incrDate
   * @param prevDate
   * @return
   */
  def readDF(paths: String, incrDate: String, prevDate: String): (DataFrame, DataFrame, DataFrame) = {

    val fileDate = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)

    val dfOpen = DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, DataSets.OPEN, DataSets.DAILY_MODE, incrDate, "53699_" + "OPEN_" + fileDate + ".txt", "true", ";")
    val dfClick = DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, DataSets.CLICK, DataSets.DAILY_MODE, incrDate, "53699_" + "CLICK_" + fileDate + ".txt", "true", ";")

    if (paths != null) {

      (dfOpen, dfClick, null)
    } else {

      val dfFullCPOTPart1 = DataReader.getDataFrame(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART1, DataSets.FULL_MERGE_MODE, prevDate)

      (dfOpen, dfClick, dfFullCPOTPart1)
    }
  }

}
