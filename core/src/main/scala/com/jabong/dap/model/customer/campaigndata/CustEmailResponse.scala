package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.EmailResponseVariables
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.MergeUtils
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.customer.schema.CustEmailSchema
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
 * Created by samathashetty on 13/10/15.
 */
object CustEmailResponse extends Logging {
  def start(params: ParamInfo) = {
    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getTodayDate(TimeConstants.DATE_FORMAT))
    val saveMode = params.saveMode
    val path = OptionUtils.getOptValue(params.path)
    emailResponse(incrDate, saveMode, path)
  }

  val open_segment: String => String = (value: String) => {
    //TODO: currently we aren't  looking at the subscriber emails to get the update date
    /*select email, dg_start_date as update_date from (
    select nl.SUBSCRIBER_EMAIL as email,nl.dg_start_date,
    row_number() over(partition by nl.SUBSCRIBER_EMAIL order by nl.dg_start_date desc) as rno
    from ${RGCM_CAMP_LOGIC_READ_SCHEMA}.dg_mag_newsl_subscr nl
    where nl.dg_end_date > sysdate ) where rno=1;*/
    if (null == value)
      "-1"
    else {

      val date = TimeUtils.getDate(value, TimeConstants.DD_MMM_YYYY_HH_MM_SS)
      val time4mToday = TimeUtils.daysFromToday(date)
      val segment = {

        if (0 > time4mToday && time4mToday <= 15) {
          15
        }
        else if (15 > time4mToday && time4mToday <= 30) {
          30
        }
        else if (30 > time4mToday && time4mToday <= 60) {
          60
        }
        else if (60 > time4mToday && time4mToday <= 120) {
          120
        }
        else if (120 > time4mToday && time4mToday <= 180) {
          180
        }
        else if (180 > time4mToday && time4mToday <= 210) {
          210
        }
        else if (210 > time4mToday && time4mToday <= 240) {
          240
        }
        else if (240 > time4mToday && time4mToday <= 270) {
          270
        }
        else if (270 > time4mToday && time4mToday <= 300) {
          300
        }
        else if (300 > time4mToday && time4mToday <= 330) {
          330
        }
        else if (330 > time4mToday && time4mToday <= 360) {
          360
        }
        else if (time4mToday > 360) {
          "NO"
        }
      }
      (segment.toString)
    }
  }

  val opens = (opens: Integer, clicks: Integer) => {
    if(opens == null ){
      (clicks)
    }else{
      (opens)
    }

  }


  val openDate = (opens: String, clickDate: String) => {
    if(opens == null ){
      (clickDate)
    }else{
      (opens)
    }
  }

  val findOpen = udf(opens)

  val findOpenDate = udf(openDate)

  def emailResponse(incrDate: String, saveMode: String, path: String) = {
    val before7daysString = TimeUtils.getDateAfterNDays(-7, TimeConstants.DATE_FORMAT, incrDate)

    val before15daysString = TimeUtils.getDateAfterNDays(-15, TimeConstants.DATE_FORMAT, incrDate)

    val before30daysString = TimeUtils.getDateAfterNDays(-30, TimeConstants.DATE_FORMAT, incrDate)

    val yesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT, incrDate)

    val savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES,
      DataSets.CUST_EMAIL_RESPONSE, DataSets.FULL_MERGE_MODE, incrDate)

    if (DataWriter.canWrite(saveMode, savePath)) {

      val incrDf = readDataFrame(incrDate, DataSets.DAILY_MODE)
      val yesterdayDf = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES,
        DataSets.CUST_EMAIL_RESPONSE, DataSets.FULL_MERGE_MODE, yesterday)

      val days7Df = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES,
        DataSets.CUST_EMAIL_RESPONSE, DataSets.DAILY_MODE, before7daysString)
      val days15Df = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES,
        DataSets.CUST_EMAIL_RESPONSE, DataSets.DAILY_MODE, before15daysString)
      val days30Df = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES,
        DataSets.CUST_EMAIL_RESPONSE, DataSets.DAILY_MODE, before30daysString)

      //   val (yesIncrSelectDf: DataFrame) = joinDataFrames(incrDf, yesterdayDf)

      val dtFunc2 = udf(open_segment)

      val resultSet = effectiveDFFull(incrDf, yesterdayDf, days7Df, days15Df, days30Df)
        .withColumn(EmailResponseVariables.OPEN_SEGMENT, dtFunc2(col(EmailResponseVariables.LAST_OPEN_DATE)))

      //TODO: replace customer_id with the UUID generator
      def result = resultSet.select(
        col(EmailResponseVariables.CUSTOMER_ID),
        col(EmailResponseVariables.OPEN_SEGMENT),
        col(EmailResponseVariables.OPEN_7DAYS),
        col(EmailResponseVariables.OPEN_15DAYS),
        col(EmailResponseVariables.OPEN_30DAYS),
        col(EmailResponseVariables.CLICK_7DAYS),
        col(EmailResponseVariables.CLICK_15DAYS),
        col(EmailResponseVariables.CLICK_30DAYS),
        Udf.outputCsvFormat(col(EmailResponseVariables.LAST_OPEN_DATE)) as EmailResponseVariables.LAST_OPEN_DATE,
        Udf.outputCsvFormat(col(EmailResponseVariables.LAST_CLICK_DATE)) as EmailResponseVariables.LAST_CLICK_DATE,
        col(EmailResponseVariables.OPENS_LIFETIME),
        col(EmailResponseVariables.CLICKS_LIFETIME)
      )

      DataWriter.writeParquet(result, savePath, saveMode)

      val savePathIncr = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES,
        DataSets.CUST_EMAIL_RESPONSE, DataSets.DAILY_MODE, incrDate)
      if (DataWriter.canWrite(savePathIncr, saveMode)) {
        DataWriter.writeParquet(incrDf, savePathIncr, saveMode)
      }

      val fileName = TimeUtils.getTodayDate(TimeConstants.YYYYMMDD) + "_CUST_EMAIL_RESPONSE"

      DataWriter.writeCsv(result, DataSets.VARIABLES, DataSets.CUST_EMAIL_RESPONSE, DataSets.DAILY_MODE, incrDate, fileName, saveMode, "true", ";")

    }

  }


  def effectiveDFFull(incremental: DataFrame, full: DataFrame, effective7: DataFrame, effective15: DataFrame, effective30: DataFrame): DataFrame = {

    if (incremental == null) {
      logger.error("Incremental DataFrame is null, returning full")
      return (full)
    }

    val joined_7_15 = MergeUtils.joinOldAndNewDF(effective15, CustEmailSchema.reqCsvDf,
      effective7, CustEmailSchema.reqCsvDf, EmailResponseVariables.CUSTOMER_ID)


    val joined_7_15_summary = joined_7_15.select(
      coalesce(col(EmailResponseVariables.CUSTOMER_ID), col(MergeUtils.NEW_ + EmailResponseVariables.CUSTOMER_ID)) as EmailResponseVariables.CUSTOMER_ID,
      col(EmailResponseVariables.CLICKS_TODAY) as EmailResponseVariables.CLICK_7DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_TODAY) as EmailResponseVariables.CLICK_15DAYS,
    col(EmailResponseVariables.OPENS_TODAY) as EmailResponseVariables.OPEN_7DAYS,
    col(MergeUtils.NEW_ + EmailResponseVariables.OPENS_TODAY) as EmailResponseVariables.OPEN_15DAYS)
      .withColumn(EmailResponseVariables.OPEN_7DAYS, findOpen(col(EmailResponseVariables.OPEN_7DAYS), col(EmailResponseVariables.CLICK_7DAYS)))
      .withColumn(EmailResponseVariables.OPEN_15DAYS, findOpen(col(EmailResponseVariables.OPEN_15DAYS), col(EmailResponseVariables.CLICK_15DAYS)))


    val joined_7_15_30 = MergeUtils.joinOldAndNewDF(effective30, CustEmailSchema.reqCsvDf,
      joined_7_15_summary, CustEmailSchema.effective7_15Schema, EmailResponseVariables.CUSTOMER_ID)
      .na.fill(Map(
      EmailResponseVariables.CLICK_15DAYS -> 0,
      EmailResponseVariables.OPEN_15DAYS -> 0,
      EmailResponseVariables.CLICK_7DAYS -> 0,
      EmailResponseVariables.OPEN_7DAYS -> 0,
      MergeUtils.NEW_ + EmailResponseVariables.CLICKS_TODAY -> 0,
      MergeUtils.NEW_ + EmailResponseVariables.OPENS_TODAY -> 0
    ))

    val joined_7_15_30_summary = joined_7_15_30.select(
      coalesce(col(MergeUtils.NEW_ + EmailResponseVariables.CUSTOMER_ID), col(EmailResponseVariables.CUSTOMER_ID))
        as EmailResponseVariables.CUSTOMER_ID,
      col(EmailResponseVariables.CLICK_7DAYS) as EmailResponseVariables.CLICK_7DAYS,
      col(EmailResponseVariables.CLICK_15DAYS) as EmailResponseVariables.CLICK_15DAYS,
      col(EmailResponseVariables.OPEN_7DAYS) as EmailResponseVariables.OPEN_7DAYS,
      col(EmailResponseVariables.OPEN_15DAYS) as EmailResponseVariables.OPEN_15DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_TODAY) as EmailResponseVariables.CLICK_30DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.OPENS_TODAY) as EmailResponseVariables.OPEN_30DAYS)
      .withColumn(EmailResponseVariables.OPEN_30DAYS, findOpen(col(EmailResponseVariables.OPEN_30DAYS), col(EmailResponseVariables.CLICK_30DAYS)))

    val joinedIncr = MergeUtils.joinOldAndNewDF(incremental, CustEmailSchema.resCustomerEmail,
      joined_7_15_30_summary, CustEmailSchema.resCustomerEmail, EmailResponseVariables.CUSTOMER_ID)
      .na.fill(Map(
      EmailResponseVariables.CLICK_15DAYS -> 0,
      EmailResponseVariables.OPEN_15DAYS -> 0,
      EmailResponseVariables.CLICK_7DAYS -> 0,
      EmailResponseVariables.OPEN_7DAYS -> 0,
      EmailResponseVariables.CLICK_30DAYS -> 0,
      EmailResponseVariables.OPEN_30DAYS -> 0,
      MergeUtils.NEW_ + EmailResponseVariables.CLICKS_TODAY -> 0,
      MergeUtils.NEW_ + EmailResponseVariables.OPENS_TODAY -> 0
    ))

    val joinedIncrSummary = joinedIncr.select(
      coalesce(col(MergeUtils.NEW_ + EmailResponseVariables.CUSTOMER_ID), col(EmailResponseVariables.CUSTOMER_ID)) as EmailResponseVariables.CUSTOMER_ID,
      col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_TODAY) - col(EmailResponseVariables.CLICK_7DAYS) as EmailResponseVariables.CLICK_7DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.OPENS_TODAY) - col(EmailResponseVariables.OPEN_7DAYS) as EmailResponseVariables.OPEN_7DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_TODAY) - col(EmailResponseVariables.CLICK_15DAYS) as EmailResponseVariables.CLICK_15DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.OPENS_TODAY) - col(EmailResponseVariables.OPEN_15DAYS) as EmailResponseVariables.OPEN_15DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_TODAY) - col(EmailResponseVariables.CLICK_30DAYS) as EmailResponseVariables.CLICK_30DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.OPENS_TODAY) - col(EmailResponseVariables.OPEN_30DAYS) as EmailResponseVariables.OPEN_30DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_TODAY) as EmailResponseVariables.CLICKS_LIFETIME,
      col(MergeUtils.NEW_ + EmailResponseVariables.OPENS_TODAY) as EmailResponseVariables.OPENS_LIFETIME,
      col(MergeUtils.NEW_ + EmailResponseVariables.LAST_CLICK_DATE) as EmailResponseVariables.LAST_CLICK_DATE,
      col(MergeUtils.NEW_ + EmailResponseVariables.LAST_OPEN_DATE) as EmailResponseVariables.LAST_OPEN_DATE
    )

    val fullDf = MergeUtils.joinOldAndNewDF(joinedIncrSummary, CustEmailSchema.effective_Smry_Schema, full, CustEmailSchema.resCustomerEmail, EmailResponseVariables.CUSTOMER_ID)
      .na.fill(
      Map(
        EmailResponseVariables.CLICKS_LIFETIME -> 0,
        EmailResponseVariables.OPENS_LIFETIME -> 0,
        EmailResponseVariables.CLICK_7DAYS -> 0,
        EmailResponseVariables.CLICK_15DAYS -> 0,
        EmailResponseVariables.CLICK_30DAYS -> 0,
        EmailResponseVariables.OPEN_7DAYS -> 0,
        EmailResponseVariables.OPEN_15DAYS -> 0,
        EmailResponseVariables.OPEN_30DAYS -> 0

      ))

    val fullSummary = fullDf.select(
      coalesce(col(EmailResponseVariables.CUSTOMER_ID), col(MergeUtils.NEW_ + EmailResponseVariables.CUSTOMER_ID)) as EmailResponseVariables.CUSTOMER_ID,
      col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_LIFETIME).cast(IntegerType) + col(EmailResponseVariables.CLICKS_LIFETIME).cast(IntegerType) as EmailResponseVariables.CLICKS_LIFETIME,
      col(MergeUtils.NEW_ + EmailResponseVariables.OPENS_LIFETIME) + col(EmailResponseVariables.OPENS_LIFETIME) as EmailResponseVariables.OPENS_LIFETIME,
      when(col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_LIFETIME) > 0, col(MergeUtils.NEW_ + EmailResponseVariables.LAST_CLICK_DATE))
        .otherwise(
          when(col(EmailResponseVariables.LAST_CLICK_DATE) !== null, col(EmailResponseVariables.LAST_CLICK_DATE))
            .otherwise("01-Jan-2001 00:00:00")) as EmailResponseVariables.LAST_CLICK_DATE,
      when(col(MergeUtils.NEW_ + EmailResponseVariables.OPENS_LIFETIME) > 0,
        Udf.getLatestDate(col(MergeUtils.NEW_ + EmailResponseVariables.LAST_OPEN_DATE),col(EmailResponseVariables.LAST_OPEN_DATE)))
        .otherwise(
          Udf.getLatestDate(col(MergeUtils.NEW_ + EmailResponseVariables.LAST_CLICK_DATE),col(EmailResponseVariables.LAST_CLICK_DATE))

    )
        as EmailResponseVariables.LAST_OPEN_DATE,
      col(EmailResponseVariables.CLICK_7DAYS) + col(MergeUtils.NEW_ + EmailResponseVariables.CLICK_7DAYS) as EmailResponseVariables.CLICK_7DAYS,
      col(EmailResponseVariables.CLICK_15DAYS) + col(MergeUtils.NEW_ + EmailResponseVariables.CLICK_15DAYS) as EmailResponseVariables.CLICK_15DAYS,
      col(EmailResponseVariables.CLICK_30DAYS) + col(MergeUtils.NEW_ + EmailResponseVariables.CLICK_30DAYS) as EmailResponseVariables.CLICK_30DAYS,
      col(EmailResponseVariables.OPEN_7DAYS) + col(MergeUtils.NEW_ + EmailResponseVariables.OPEN_7DAYS) as EmailResponseVariables.OPEN_7DAYS,
      col(EmailResponseVariables.OPEN_15DAYS) + col(MergeUtils.NEW_ + EmailResponseVariables.OPEN_15DAYS) as EmailResponseVariables.OPEN_15DAYS,
      col(EmailResponseVariables.OPEN_30DAYS) + col(MergeUtils.NEW_ + EmailResponseVariables.OPEN_30DAYS) as EmailResponseVariables.OPEN_30DAYS

    )
    (fullSummary)
  }

  def readDataFrame(date: String, mode: String): (DataFrame) = {
    val formattedDate = TimeUtils.changeDateFormat(date, TimeConstants.DATE_FORMAT, TimeConstants.YYYYMMDD)

    val filename = "53699_CLICK_" + formattedDate + ".txt"

    val dfClickData = DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, DataSets.CLICK, DataSets.DAILY_MODE, date, filename, "true", ";")

    val aggClickData = reduce(dfClickData, EmailResponseVariables.LAST_CLICK_DATE, EmailResponseVariables.CLICKS_TODAY)

    val openFilename = "53699_OPEN_" + formattedDate + ".txt"

    val dfOpenData = DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, DataSets.OPEN, DataSets.DAILY_MODE, date, openFilename, "true", ";")

    val aggOpenData = reduce(dfOpenData, EmailResponseVariables.LAST_OPEN_DATE, EmailResponseVariables.OPENS_TODAY)

    val joinedDf = MergeUtils.joinOldAndNewDF(aggClickData, CustEmailSchema.effectiveSchema,
      aggOpenData, CustEmailSchema.effectiveSchema, EmailResponseVariables.CUSTOMER_ID)
      .select(coalesce(col(EmailResponseVariables.CUSTOMER_ID), col(MergeUtils.NEW_ + EmailResponseVariables.CUSTOMER_ID)) as EmailResponseVariables.CUSTOMER_ID,
        col(EmailResponseVariables.LAST_OPEN_DATE) as EmailResponseVariables.LAST_OPEN_DATE,
        col(EmailResponseVariables.OPENS_TODAY) as EmailResponseVariables.OPENS_TODAY,
        col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_TODAY) as EmailResponseVariables.CLICKS_TODAY,
        col(MergeUtils.NEW_ + EmailResponseVariables.LAST_CLICK_DATE) as EmailResponseVariables.LAST_CLICK_DATE)
      .withColumn(EmailResponseVariables.OPENS_TODAY, findOpen(col(EmailResponseVariables.OPENS_TODAY), col(EmailResponseVariables.CLICKS_TODAY)))

    (joinedDf)
  }

  def reduce(df: DataFrame, evenDateType: String, eventNumType: String): DataFrame = {
    df.groupBy(EmailResponseVariables.CUSTOMER_ID)
      .agg(max(EmailResponseVariables.EVENT_CAPTURED_DT) as evenDateType,
        count(EmailResponseVariables.CUSTOMER_ID) as eventNumType)
  }
}
