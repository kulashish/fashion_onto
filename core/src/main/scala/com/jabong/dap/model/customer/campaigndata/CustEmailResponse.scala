package com.jabong.dap.model.customer.campaigndata

import java.util.Date

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.EmailResponseVariables
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.MergeUtils
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.customer.schema.CustEmailSchema
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ DateType, IntegerType }

/**
 * Created by samathashetty on 13/10/15.
 */
object CustEmailResponse extends Logging {

  def start(params: ParamInfo) = {
    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getTodayDate(TimeConstants.DATE_FORMAT_FOLDER))
    val saveMode = params.saveMode
    val path = OptionUtils.getOptValue(params.path)
    emailResponse(incrDate, saveMode, path)
  }

  val open_segment: String => Int = (value: String) => {
    if (null == value)
      -1
    else {

      val date = TimeUtils.getDate(value, "dd-MMM-yyyy HH:mm:ss")
      val time4mToday = TimeUtils.daysFromToday(date)
      var segment = -1
      if (time4mToday < 15) {
        segment = 15
      } else {
        segment = (time4mToday / 30 + 1) * 30
      }
      segment

    }
  }

  def emailResponse(incrDate: String, saveMode: String, path: String) = {
    val before7daysString = TimeUtils.getDateAfterNDays(-7, TimeConstants.DATE_FORMAT, incrDate)

    val before15daysString = TimeUtils.getDateAfterNDays(-15, TimeConstants.DATE_FORMAT, incrDate)

    val before30daysString = TimeUtils.getDateAfterNDays(-30, TimeConstants.DATE_FORMAT, incrDate)

    val yesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT, incrDate)

    val savePathI = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES,
      DataSets.CUST_EMAIL_RESPONSE, DataSets.FULL_MERGE_MODE, incrDate)

    if (DataWriter.canWrite(saveMode, savePathI)) {

      val incrDf = readDataFrame(incrDate, DataSets.DAILY_MODE)
      val yesterdayDf = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES,
        DataSets.CUST_EMAIL_RESPONSE, DataSets.FULL_MERGE_MODE, yesterday)

      val days7Df = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES,
        DataSets.CUST_EMAIL_RESPONSE, DataSets.DAILY_MODE, before7daysString)
      val days15Df = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES,
        DataSets.CUST_EMAIL_RESPONSE, DataSets.DAILY_MODE, before15daysString)
      val days30Df = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES,
        DataSets.CUST_EMAIL_RESPONSE, DataSets.DAILY_MODE, before30daysString)

      // Joining previous and current DFs to get the total clicks and the opens.
      // clicks lifetime = clicks today + click lifetime of yesterday
      val yesIncrDf = MergeUtils.joinOldAndNewDF(incrDf, CustEmailSchema.effectiveSchema, yesterdayDf, CustEmailSchema.effectiveSchema, EmailResponseVariables.CUSTOMER_ID)
        .na.fill(
          Map(
            EmailResponseVariables.CLICKS_LIFETIME -> 0,
            EmailResponseVariables.OPENS_LIFETIME -> 0
          ))

      val yesIncrSelectDf = yesIncrDf.select(
        coalesce(col(EmailResponseVariables.CUSTOMER_ID), col(MergeUtils.NEW_ + EmailResponseVariables.CUSTOMER_ID)) as EmailResponseVariables.CUSTOMER_ID,
        col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_TODAY).cast(IntegerType) + col(EmailResponseVariables.CLICKS_LIFETIME).cast(IntegerType) as EmailResponseVariables.CLICKS_LIFETIME,
        col(MergeUtils.NEW_ + EmailResponseVariables.OPENS_TODAY) + col(EmailResponseVariables.OPENS_LIFETIME) as EmailResponseVariables.OPENS_LIFETIME,
        when(col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_TODAY) > 0, col(MergeUtils.NEW_ + EmailResponseVariables.LAST_CLICK_DATE))
          .otherwise(
            when(col(EmailResponseVariables.LAST_CLICK_DATE) > 0, col(EmailResponseVariables.LAST_CLICK_DATE))
              .otherwise("01-Jan-2001 00:00:00")) as EmailResponseVariables.LAST_CLICK_DATE,
        when(col(MergeUtils.NEW_ + EmailResponseVariables.OPENS_TODAY) > 0, col(MergeUtils.NEW_ + EmailResponseVariables.LAST_OPEN_DATE))
          .otherwise(
            when(col(EmailResponseVariables.LAST_OPEN_DATE) > 0, col(EmailResponseVariables.LAST_OPEN_DATE))
              .otherwise("01-Jan-2001 00:00:00")) as EmailResponseVariables.LAST_OPEN_DATE
      )

      val dtFunc2 = udf(open_segment)
      val resultSet = effectiveDFFull(yesIncrSelectDf, days7Df, days15Df, days30Df)
        .withColumn(EmailResponseVariables.OPEN_SEGMENT, dtFunc2(col(EmailResponseVariables.LAST_OPEN_DATE)))

      //TODO: replace customer_id with the UUID generator
      def result = resultSet.select(col(EmailResponseVariables.CUSTOMER_ID),
        col(EmailResponseVariables.OPEN_SEGMENT),
        col(EmailResponseVariables.OPEN_7DAYS),
        col(EmailResponseVariables.OPEN_15DAYS),
        col(EmailResponseVariables.OPEN_30DAYS),
        col(EmailResponseVariables.CLICK_7DAYS),
        col(EmailResponseVariables.CLICK_15DAYS),
        col(EmailResponseVariables.CLICK_30DAYS),
        col(EmailResponseVariables.LAST_OPEN_DATE),
        col(EmailResponseVariables.LAST_CLICK_DATE),
        col(EmailResponseVariables.OPENS_LIFETIME),
        col(EmailResponseVariables.CLICKS_LIFETIME)
      )
      val fileName = "53699_28344_" + TimeUtils.getTodayDate(TimeConstants.YYYYMMDD) + "CUST_EMAIL_RESPONSE"

      DataWriter.writeCsv(result, DataSets.VARIABLES, DataSets.CUST_EMAIL_RESPONSE, DataSets.DAILY_MODE, incrDate, fileName, saveMode, "true", ";")

    }

  }

  def effectiveDFFull(incremental: DataFrame, effective7: DataFrame, effective15: DataFrame, effective30: DataFrame): DataFrame = {
    //send DataFrames after using reduce

    val joined_7_15 = MergeUtils.joinOldAndNewDF(effective15, CustEmailSchema.resCustomerEmail,
      effective7, CustEmailSchema.resCustomerEmail, EmailResponseVariables.CUSTOMER_ID)
      .na.fill(Map(
        EmailResponseVariables.CLICKS_LIFETIME -> 0,
        EmailResponseVariables.OPENS_LIFETIME -> 0,
        MergeUtils.NEW_ + EmailResponseVariables.CLICKS_LIFETIME -> 0,
        MergeUtils.NEW_ + EmailResponseVariables.OPENS_LIFETIME -> 0
      ))

    val joined_7_15_summary = joined_7_15.select(
      coalesce(col(EmailResponseVariables.CUSTOMER_ID), col(MergeUtils.NEW_ + EmailResponseVariables.CUSTOMER_ID)) as EmailResponseVariables.CUSTOMER_ID,
      col(EmailResponseVariables.CLICKS_LIFETIME) as EmailResponseVariables.CLICK_7DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_LIFETIME) as EmailResponseVariables.CLICK_15DAYS,
      col(EmailResponseVariables.OPENS_LIFETIME) as EmailResponseVariables.OPEN_7DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.OPENS_LIFETIME) as EmailResponseVariables.OPEN_15DAYS)

    val joined_7_15_30 = MergeUtils.joinOldAndNewDF(effective30, CustEmailSchema.resCustomerEmail,
      joined_7_15_summary, CustEmailSchema.resCustomerEmail, EmailResponseVariables.CUSTOMER_ID)
      .na.fill(Map(
        EmailResponseVariables.CLICK_15DAYS -> 0,
        EmailResponseVariables.OPEN_15DAYS -> 0,
        EmailResponseVariables.CLICK_7DAYS -> 0,
        EmailResponseVariables.OPEN_7DAYS -> 0,
        MergeUtils.NEW_ + EmailResponseVariables.CLICKS_LIFETIME -> 0,
        MergeUtils.NEW_ + EmailResponseVariables.OPENS_LIFETIME -> 0
      ))

    val joined_7_15_30_summary = joined_7_15_30.select(
      coalesce(col(MergeUtils.NEW_ + EmailResponseVariables.CUSTOMER_ID), col(EmailResponseVariables.CUSTOMER_ID))
        as EmailResponseVariables.CUSTOMER_ID,
      col(EmailResponseVariables.CLICK_7DAYS) as EmailResponseVariables.CLICK_7DAYS,
      col(EmailResponseVariables.CLICK_15DAYS) as EmailResponseVariables.CLICK_15DAYS,
      col(EmailResponseVariables.OPEN_7DAYS) as EmailResponseVariables.OPEN_7DAYS,
      col(EmailResponseVariables.OPEN_15DAYS) as EmailResponseVariables.OPEN_15DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_LIFETIME) as EmailResponseVariables.CLICK_30DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.OPENS_LIFETIME) as EmailResponseVariables.OPEN_30DAYS)

    val joinedAll = MergeUtils.joinOldAndNewDF(incremental, CustEmailSchema.resCustomerEmail,
      joined_7_15_30_summary, CustEmailSchema.resCustomerEmail, EmailResponseVariables.CUSTOMER_ID)
      .na.fill(Map(
        EmailResponseVariables.CLICK_15DAYS -> 0,
        EmailResponseVariables.OPEN_15DAYS -> 0,
        EmailResponseVariables.CLICK_7DAYS -> 0,
        EmailResponseVariables.OPEN_7DAYS -> 0,
        EmailResponseVariables.CLICK_30DAYS -> 0,
        EmailResponseVariables.OPEN_30DAYS -> 0,
        MergeUtils.NEW_ + EmailResponseVariables.CLICKS_LIFETIME -> 0,
        MergeUtils.NEW_ + EmailResponseVariables.OPENS_LIFETIME -> 0
      ))

    val joinedAllSummary = joinedAll.select(
      coalesce(col(MergeUtils.NEW_ + EmailResponseVariables.CUSTOMER_ID), col(EmailResponseVariables.CUSTOMER_ID)) as EmailResponseVariables.CUSTOMER_ID,
      col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_LIFETIME) - col(EmailResponseVariables.CLICK_7DAYS) as EmailResponseVariables.CLICK_7DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.OPENS_LIFETIME) - col(EmailResponseVariables.OPEN_7DAYS) as EmailResponseVariables.OPEN_7DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_LIFETIME) - col(EmailResponseVariables.CLICK_15DAYS) as EmailResponseVariables.CLICK_15DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.OPENS_LIFETIME) - col(EmailResponseVariables.OPEN_15DAYS) as EmailResponseVariables.OPEN_15DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_LIFETIME) - col(EmailResponseVariables.CLICK_30DAYS) as EmailResponseVariables.CLICK_30DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.OPENS_LIFETIME) - col(EmailResponseVariables.OPEN_30DAYS) as EmailResponseVariables.OPEN_30DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_LIFETIME) as EmailResponseVariables.CLICKS_LIFETIME,
      col(MergeUtils.NEW_ + EmailResponseVariables.OPENS_LIFETIME) as EmailResponseVariables.OPENS_LIFETIME,
      col(MergeUtils.NEW_ + EmailResponseVariables.LAST_CLICK_DATE) as EmailResponseVariables.LAST_CLICK_DATE,
      col(MergeUtils.NEW_ + EmailResponseVariables.LAST_OPEN_DATE) as EmailResponseVariables.LAST_OPEN_DATE
    )

    joinedAllSummary
  }

  def readDataFrame(date: String, mode: String): (DataFrame) = {
    val formattedDate = TimeUtils.changeDateFormat(date, TimeConstants.DATE_FORMAT, TimeConstants.YYYYMMDD)

    val filename = "53699_CLICK_" + formattedDate + ".txt"

    val dfClickData = DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, DataSets.CLICK, DataSets.DAILY_MODE, date, filename, "true", ";")

    val aggClickData = reduce(dfClickData, EmailResponseVariables.LAST_CLICK_DATE, EmailResponseVariables.CLICKS_TODAY)

    val openFilename = "53699_OPEN_" + formattedDate + ".txt"

    val dfOpenData = DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, DataSets.OPEN, DataSets.DAILY_MODE, date, openFilename, "true", ";")

    val aggOpenData = reduce(dfOpenData, EmailResponseVariables.LAST_OPEN_DATE, EmailResponseVariables.OPENS_TODAY)

    val joinedDf = MergeUtils.joinOldAndNewDF(aggClickData, aggOpenData, EmailResponseVariables.CUSTOMER_ID)
      .select(coalesce(col(EmailResponseVariables.CUSTOMER_ID), col(MergeUtils.NEW_ + EmailResponseVariables.CUSTOMER_ID)) as EmailResponseVariables.CUSTOMER_ID,
        col(EmailResponseVariables.LAST_OPEN_DATE) as EmailResponseVariables.LAST_OPEN_DATE,
        col(EmailResponseVariables.OPENS_TODAY) as EmailResponseVariables.OPENS_TODAY,
        col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_TODAY) as EmailResponseVariables.CLICKS_TODAY,
        col(MergeUtils.NEW_ + EmailResponseVariables.LAST_CLICK_DATE) as EmailResponseVariables.LAST_CLICK_DATE)

    (joinedDf)
  }

  def reduce(df: DataFrame, evenDateType: String, eventNumType: String): DataFrame = {
    df.groupBy(EmailResponseVariables.CUSTOMER_ID)
      .agg(max(EmailResponseVariables.EVENT_CAPTURED_DT) as evenDateType,
        count(EmailResponseVariables.CUSTOMER_ID) as eventNumType)
  }
}
