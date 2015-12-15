package com.jabong.dap.model.responsys.campaigndata

import com.jabong.dap.common.{ Utils, Spark }
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.CustomerVariables
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.{ Udf, UdfUtils }
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.customer.schema.CustVarSchema
import com.jabong.dap.model.dataFeeds.DataFeedsModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row }

import scala.collection.mutable.{ ArrayBuffer, HashMap }

/**
 * Created by raghu on 13/10/15.
 */
object CustomerPreferredTimeslotPart1 extends DataFeedsModel {

  def canProcess(incrDate: String, saveMode: String): Boolean = {
    val pathCustomerPreferredTimeslotPart1Full = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART1, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.canWrite(saveMode, pathCustomerPreferredTimeslotPart1Full)
  }

  /**
   *
   * @param paths
   * @param incrDate
   * @param prevDate
   * @return
   */
  def readDF(incrDate: String, prevDate: String, paths: String): HashMap[String, DataFrame] = {
    val dfMap = new HashMap[String, DataFrame]()

    val fileDate = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)

    val dfOpenIncr = DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, DataSets.OPEN, DataSets.DAILY_MODE, incrDate, "53699_" + "OPEN_" + fileDate + ".txt", "true", ";")
    dfMap.put("openIncr", dfOpenIncr)
    val dfClickIncr = DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, DataSets.CLICK, DataSets.DAILY_MODE, incrDate, "53699_" + "CLICK_" + fileDate + ".txt", "true", ";")
    dfMap.put("clickIncr", dfClickIncr)

    if (null == paths) {
      val dfCPOTPart1PrevFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART1, DataSets.FULL_MERGE_MODE, prevDate)
      dfMap.put("cpotPart1PrevFull", dfCPOTPart1PrevFull)
    }
    dfMap
  }

  def process(dfMap: HashMap[String, DataFrame]): HashMap[String, DataFrame] = {

    var dfCPOTPart1Incr = getIncCPOTPart1(dfMap("openIncr"), dfMap("clickIncr"))
    var dfCPOTPart1Full = dfCPOTPart1Incr

    val dfCPOTPart1PrevFull = dfMap.getOrElse("cpotPart1PrevFull", null)

    if (null != dfCPOTPart1PrevFull) {

      val dfIncrVarBC = Spark.getContext().broadcast(dfCPOTPart1Incr).value
      //join old and new data frame
      val joinDF = dfCPOTPart1PrevFull.join(dfIncrVarBC, dfCPOTPart1PrevFull(CustomerVariables.CUSTOMER_ID) === dfIncrVarBC(CustomerVariables.CUSTOMER_ID), SQL.FULL_OUTER)
      //MergeUtils.joinOldAndNewDF(dfIncrVarBC, dfPrevFullCPOTPart1, CustomerVariables.CUSTOMER_ID)

      val dfFull = joinDF.select(
        coalesce(dfIncrVarBC(CustomerVariables.CUSTOMER_ID), dfCPOTPart1PrevFull(CustomerVariables.CUSTOMER_ID)) as CustomerVariables.CUSTOMER_ID,
        Udf.addInt(dfIncrVarBC(CustomerVariables.OPEN_0), dfCPOTPart1PrevFull(CustomerVariables.OPEN_0)) as CustomerVariables.OPEN_0,
        Udf.addInt(dfIncrVarBC(CustomerVariables.OPEN_1), dfCPOTPart1PrevFull(CustomerVariables.OPEN_1)) as CustomerVariables.OPEN_1,
        Udf.addInt(dfIncrVarBC(CustomerVariables.OPEN_2), dfCPOTPart1PrevFull(CustomerVariables.OPEN_2)) as CustomerVariables.OPEN_2,
        Udf.addInt(dfIncrVarBC(CustomerVariables.OPEN_3), dfCPOTPart1PrevFull(CustomerVariables.OPEN_3)) as CustomerVariables.OPEN_3,
        Udf.addInt(dfIncrVarBC(CustomerVariables.OPEN_4), dfCPOTPart1PrevFull(CustomerVariables.OPEN_4)) as CustomerVariables.OPEN_4,
        Udf.addInt(dfIncrVarBC(CustomerVariables.OPEN_5), dfCPOTPart1PrevFull(CustomerVariables.OPEN_5)) as CustomerVariables.OPEN_5,
        Udf.addInt(dfIncrVarBC(CustomerVariables.OPEN_6), dfCPOTPart1PrevFull(CustomerVariables.OPEN_6)) as CustomerVariables.OPEN_6,
        Udf.addInt(dfIncrVarBC(CustomerVariables.OPEN_7), dfCPOTPart1PrevFull(CustomerVariables.OPEN_7)) as CustomerVariables.OPEN_7,
        Udf.addInt(dfIncrVarBC(CustomerVariables.OPEN_8), dfCPOTPart1PrevFull(CustomerVariables.OPEN_8)) as CustomerVariables.OPEN_8,
        Udf.addInt(dfIncrVarBC(CustomerVariables.OPEN_9), dfCPOTPart1PrevFull(CustomerVariables.OPEN_9)) as CustomerVariables.OPEN_9,
        Udf.addInt(dfIncrVarBC(CustomerVariables.OPEN_10), dfCPOTPart1PrevFull(CustomerVariables.OPEN_10)) as CustomerVariables.OPEN_10,
        Udf.addInt(dfIncrVarBC(CustomerVariables.OPEN_11), dfCPOTPart1PrevFull(CustomerVariables.OPEN_11)) as CustomerVariables.OPEN_11,
        Udf.addInt(dfIncrVarBC(CustomerVariables.CLICK_0), dfCPOTPart1PrevFull(CustomerVariables.CLICK_0)) as CustomerVariables.CLICK_0,
        Udf.addInt(dfIncrVarBC(CustomerVariables.CLICK_1), dfCPOTPart1PrevFull(CustomerVariables.CLICK_1)) as CustomerVariables.CLICK_1,
        Udf.addInt(dfIncrVarBC(CustomerVariables.CLICK_2), dfCPOTPart1PrevFull(CustomerVariables.CLICK_2)) as CustomerVariables.CLICK_2,
        Udf.addInt(dfIncrVarBC(CustomerVariables.CLICK_3), dfCPOTPart1PrevFull(CustomerVariables.CLICK_3)) as CustomerVariables.CLICK_3,
        Udf.addInt(dfIncrVarBC(CustomerVariables.CLICK_4), dfCPOTPart1PrevFull(CustomerVariables.CLICK_4)) as CustomerVariables.CLICK_4,
        Udf.addInt(dfIncrVarBC(CustomerVariables.CLICK_5), dfCPOTPart1PrevFull(CustomerVariables.CLICK_5)) as CustomerVariables.CLICK_5,
        Udf.addInt(dfIncrVarBC(CustomerVariables.CLICK_6), dfCPOTPart1PrevFull(CustomerVariables.CLICK_6)) as CustomerVariables.CLICK_6,
        Udf.addInt(dfIncrVarBC(CustomerVariables.CLICK_7), dfCPOTPart1PrevFull(CustomerVariables.CLICK_7)) as CustomerVariables.CLICK_7,
        Udf.addInt(dfIncrVarBC(CustomerVariables.CLICK_8), dfCPOTPart1PrevFull(CustomerVariables.CLICK_8)) as CustomerVariables.CLICK_8,
        Udf.addInt(dfIncrVarBC(CustomerVariables.CLICK_9), dfCPOTPart1PrevFull(CustomerVariables.CLICK_9)) as CustomerVariables.CLICK_9,
        Udf.addInt(dfIncrVarBC(CustomerVariables.CLICK_10), dfCPOTPart1PrevFull(CustomerVariables.CLICK_10)) as CustomerVariables.CLICK_10,
        Udf.addInt(dfIncrVarBC(CustomerVariables.CLICK_11), dfCPOTPart1PrevFull(CustomerVariables.CLICK_11)) as CustomerVariables.CLICK_11)

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
      dfCPOTPart1Full = Spark.getSqlContext().createDataFrame(rowRDD, CustVarSchema.customersPreferredOrderTimeslotPart1)

      dfCPOTPart1Incr = dfCPOTPart1Full.except(dfCPOTPart1PrevFull)
    }
    val dfWrite = new HashMap[String, DataFrame]()
    dfWrite.put("dfCPOTPart1Full", dfCPOTPart1Full)
    dfWrite.put("dfCPOTPart1Incr", dfCPOTPart1Incr)
    dfWrite
  }

  def write(dfWrite: HashMap[String, DataFrame], saveMode: String, incrDate: String) = {
    val pathCustomerPreferredTimeslotPart1Full = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART1, DataSets.FULL_MERGE_MODE, incrDate)
    if (DataWriter.canWrite(saveMode, pathCustomerPreferredTimeslotPart1Full)) {
      DataWriter.writeParquet(dfWrite("dfCPOTPart1Full"), pathCustomerPreferredTimeslotPart1Full, saveMode)
    }
    val pathCustomerPreferredTimeslotPart1Incr = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART1, DataSets.DAILY_MODE, incrDate)
    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    DataWriter.writeParquet(dfWrite("dfCPOTPart1Incr"), pathCustomerPreferredTimeslotPart1Incr, saveMode)
    DataWriter.writeCsv(dfWrite("dfCPOTPart1Incr").na.fill(""), DataSets.VARIABLES, DataSets.CUSTOMER_PREFERRED_TIMESLOT_PART1, DataSets.DAILY_MODE, incrDate, fileDate + "_Customer_PREFERRED_TIMESLOT_part1", DataSets.IGNORE_SAVEMODE, "true", ";", 1)

  }

  /**
   *
   * @param dfOpen
   * @param dfClick
   * @return
   */
  def getIncCPOTPart1(dfOpen: DataFrame, dfClick: DataFrame): DataFrame = {

    val dfOpenCPOT = Utils.getCPOT(dfOpen.select("CUSTOMER_ID", CustomerVariables.EVENT_CAPTURED_DT), CustVarSchema.emailOpen, TimeConstants.DD_MMM_YYYY_HH_MM_SS)
    val dfClickCPOT = Utils.getCPOT(dfClick.select("CUSTOMER_ID", CustomerVariables.EVENT_CAPTURED_DT), CustVarSchema.emailClick, TimeConstants.DD_MMM_YYYY_HH_MM_SS)

    val dfIncCPOTPart1 = dfOpenCPOT.join(dfClickCPOT, dfOpenCPOT(CustomerVariables.CUSTOMER_ID) === dfClickCPOT(CustomerVariables.CUSTOMER_ID), SQL.FULL_OUTER)
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
}
