package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ ContactListMobileVars, CustomerVariables, EmailResponseVariables, NewsletterVariables }
import com.jabong.dap.common.schema.SchemaUtils
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.MergeUtils
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.customer.schema.CustEmailSchema
import com.jabong.dap.model.dataFeeds.DataFeedsModel
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ IntegerType, StringType }

import scala.collection.mutable

/**
 * Created by samathashetty on 13/10/15.
 */
object CustEmailResponse extends DataFeedsModel with Logging {

  var date: String = null

  override def canProcess(incrDate: String, saveMode: String): Boolean = {
    val incrSavePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_EMAIL_RESPONSE, DataSets.DAILY_MODE, incrDate)
    val fullSavePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_EMAIL_RESPONSE, DataSets.FULL, incrDate)

    DataWriter.canWrite(saveMode, incrSavePath) || DataWriter.canWrite(saveMode, fullSavePath)
  }

  override def readDF(incrDate: String, prevDate: String, paths: String): mutable.HashMap[String, DataFrame] = {
    date = incrDate
    // println("incrDate", incrDate)
    // println("date", date)
    val dfMap: mutable.HashMap[String, DataFrame] = new mutable.HashMap[String, DataFrame]()
    val fileDate = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)

    val filename = "53699_CLICK_" + fileDate + ".txt"
    val clickIncr = DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, DataSets.CLICK, DataSets.DAILY_MODE,
      incrDate, filename, "true", ";").coalesce(20)
    dfMap.put("clickIncr", clickIncr)

    val openFilename = "53699_OPEN_" + fileDate + ".txt"
    val openIncr = DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, DataSets.OPEN, DataSets.DAILY_MODE,
      incrDate, openFilename, "true", ";").coalesce(20)
    dfMap.put("openIncr", openIncr)

    val before7daysString = TimeUtils.getDateAfterNDays(-7, TimeConstants.DATE_FORMAT_FOLDER, incrDate)
    val before15daysString = TimeUtils.getDateAfterNDays(-15, TimeConstants.DATE_FORMAT_FOLDER, incrDate)
    val before30daysString = TimeUtils.getDateAfterNDays(-30, TimeConstants.DATE_FORMAT_FOLDER, incrDate)

    val days7Df = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES,
      DataSets.CUST_EMAIL_RESPONSE, DataSets.DAILY_MODE, before7daysString)
    dfMap.put("custEmailResDay7", days7Df)

    val days15Df = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES,
      DataSets.CUST_EMAIL_RESPONSE, DataSets.DAILY_MODE, before15daysString)
    dfMap.put("custEmailResDay15", days15Df)

    val days30Df = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES,
      DataSets.CUST_EMAIL_RESPONSE, DataSets.DAILY_MODE, before30daysString)
    dfMap.put("custEmailResDay30", days30Df)

    var prevFullDf: DataFrame = null
    if (null != paths) {
      val inputCsv = DataReader.getDataFrame4mCsv(paths, "true", "|").withColumnRenamed(EmailResponseVariables.CUSTOMER_ID, ContactListMobileVars.UID).coalesce(100)
      prevFullDf = SchemaUtils.addColumns(inputCsv, CustEmailSchema.effective_Smry_Schema)
    } else {
      prevFullDf = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES,
        DataSets.CUST_EMAIL_RESPONSE, DataSets.FULL_MERGE_MODE, prevDate)
    }
    dfMap.put("custEmailResPrevFull", prevFullDf)

    val dfCmrFull = CampaignInput.loadCustomerMasterData(incrDate).filter(col(CustomerVariables.EMAIL).isNotNull).filter(col(ContactListMobileVars.UID) isNotNull)
    dfMap.put("cmrFull", dfCmrFull)

    val nlSubscribers = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.NEWSLETTER_SUBSCRIPTION,
      DataSets.DAILY_MODE, incrDate)
    dfMap.put("nlsIncr", nlSubscribers)

    dfMap

  }

  override def process(dfMap: mutable.HashMap[String, DataFrame]): mutable.HashMap[String, DataFrame] = {

    val dfClickData = dfMap("clickIncr")
    val aggClickData = reduce(dfClickData, EmailResponseVariables.LAST_CLICK_DATE, EmailResponseVariables.CLICKS_TODAY)

    val dfOpenData = dfMap("openIncr")
    val aggOpenData = reduce(dfOpenData, EmailResponseVariables.LAST_OPEN_DATE, EmailResponseVariables.OPENS_TODAY)

    val outputCsvFormat = udf((s: String) => TimeUtils.changeDateFormat(s: String, TimeConstants.DD_MMM_YYYY_HH_MM_SS, TimeConstants.DATE_TIME_FORMAT))

    val custEmailResIncr = MergeUtils.joinOldAndNewDF(aggClickData, CustEmailSchema.effectiveSchema,
      aggOpenData, CustEmailSchema.effectiveSchema, EmailResponseVariables.CUSTOMER_ID)
      .select(coalesce(col(EmailResponseVariables.CUSTOMER_ID), col(MergeUtils.NEW_ + EmailResponseVariables.CUSTOMER_ID)) as ContactListMobileVars.UID,
        outputCsvFormat(col(EmailResponseVariables.LAST_OPEN_DATE)) as EmailResponseVariables.LAST_OPEN_DATE,
        col(EmailResponseVariables.OPENS_TODAY).cast(IntegerType) as EmailResponseVariables.OPENS_TODAY,
        col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_TODAY).cast(IntegerType) as EmailResponseVariables.CLICKS_TODAY,
        outputCsvFormat(col(MergeUtils.NEW_ + EmailResponseVariables.LAST_CLICK_DATE)) as EmailResponseVariables.LAST_CLICK_DATE)
      .withColumn(EmailResponseVariables.OPENS_TODAY, findOpen(col(EmailResponseVariables.OPENS_TODAY), col(EmailResponseVariables.CLICKS_TODAY))).cache()

    val prevFullDf = dfMap("custEmailResPrevFull").cache()
    val days7Df = dfMap("custEmailResDay7")
    val days15Df = dfMap("custEmailResDay15")
    val days30Df = dfMap("custEmailResDay30")

    val effectiveDf = effectiveDFFull(custEmailResIncr, prevFullDf, days7Df, days15Df, days30Df)

    val cmr = dfMap("cmrFull")

    val nlSub = dfMap("nlsIncr")

    val custEmailResFull = merge(effectiveDf, cmr, nlSub).na.fill(Map(
      EmailResponseVariables.OPEN_7DAYS -> 0,
      EmailResponseVariables.OPEN_15DAYS -> 0,
      EmailResponseVariables.OPEN_30DAYS -> 0,
      EmailResponseVariables.OPENS_LIFETIME -> 0,
      EmailResponseVariables.CLICK_7DAYS -> 0,
      EmailResponseVariables.CLICK_15DAYS -> 0,
      EmailResponseVariables.CLICK_30DAYS -> 0,
      EmailResponseVariables.CLICKS_LIFETIME -> 0)).cache()

    val udfOpenSegFn = udf((s: String, s1: String, s2: String, s3: String) => open_segment(s: String, s1: String, s2: String, s3: String))

    val custEmailResCsv = custEmailResFull.except(prevFullDf).select(
      col(ContactListMobileVars.UID),
      udfOpenSegFn(col(EmailResponseVariables.LAST_OPEN_DATE), col(NewsletterVariables.UPDATED_AT), col(EmailResponseVariables.END_DATE), lit(date))
        as EmailResponseVariables.OPEN_SEGMENT,
      col(EmailResponseVariables.OPEN_7DAYS),
      col(EmailResponseVariables.OPEN_15DAYS),
      col(EmailResponseVariables.OPEN_30DAYS),
      col(EmailResponseVariables.CLICK_7DAYS),
      col(EmailResponseVariables.CLICK_15DAYS),
      col(EmailResponseVariables.CLICK_30DAYS),
      col(EmailResponseVariables.LAST_OPEN_DATE),
      col(EmailResponseVariables.LAST_CLICK_DATE),
      col(EmailResponseVariables.OPENS_LIFETIME),
      col(EmailResponseVariables.CLICKS_LIFETIME))

    val dfResultMap: mutable.HashMap[String, DataFrame] = new mutable.HashMap[String, DataFrame]()
    dfResultMap.put("custEmailResIncr", custEmailResIncr)
    dfResultMap.put("custEmailResFull", custEmailResFull)
    dfResultMap.put("custEmailResCsv", custEmailResCsv)

    dfResultMap
  }

  override def write(dfWrite: mutable.HashMap[String, DataFrame], saveMode: String, incrDate: String) = {
    val custEmailResIncr = dfWrite("custEmailResIncr")
    val custEmailResFull = dfWrite("custEmailResFull")
    val custEmailResCsv = dfWrite("custEmailResCsv")

    val savePathIncr = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES,
      DataSets.CUST_EMAIL_RESPONSE, DataSets.DAILY_MODE, incrDate)
    val savePathFull = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES,
      DataSets.CUST_EMAIL_RESPONSE, DataSets.FULL_MERGE_MODE, incrDate)

    if (DataWriter.canWrite(savePathIncr, saveMode)) {
      DataWriter.writeParquet(custEmailResIncr, savePathIncr, saveMode)
    }

    if (DataWriter.canWrite(savePathFull, saveMode)) {
      DataWriter.writeParquet(custEmailResFull, savePathFull, saveMode)
    }

    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    DataWriter.writeCsv(custEmailResCsv, DataSets.VARIABLES, DataSets.CUST_EMAIL_RESPONSE, DataSets.DAILY_MODE, incrDate, fileDate + "_CUST_EMAIL_RESPONSE", saveMode, "true", ";", 1)

  }

  def open_segment(openValue: String, updateValue: String, endDate: String, incrDateStr: String): String = {

    //  both incrDate and update date is null
    if (null != endDate || (null == openValue && updateValue == null))
      "NO"
    else if (openValue == null) {
      //openDate is null, calculate with the update date
      val lastUpdtDate = TimeUtils.getDate(updateValue, TimeConstants.DATE_TIME_FORMAT)
      val incrDate = TimeUtils.getDate(incrDateStr, TimeConstants.DATE_FORMAT_FOLDER)
      val time4mToday = TimeUtils.daysBetweenTwoDates(lastUpdtDate, incrDate)

      val segment = {

        if (0 <= time4mToday && time4mToday <= 30) {
          "0"
        } else if (30 < time4mToday && time4mToday <= 60) {
          "X"
        } else if (60 < time4mToday && time4mToday <= 120) {
          "X1"
        } else if (120 < time4mToday && time4mToday <= 180) {
          "X2"
        } else {
          "NO"
        }
      }
      segment

    } else {
      //open date is not null, take that for calculation
      val incrDate = TimeUtils.getDate(incrDateStr, TimeConstants.DATE_FORMAT_FOLDER)
      val lastOpenDate = TimeUtils.getDate(openValue, TimeConstants.DATE_TIME_FORMAT)

      val time4mToday = TimeUtils.daysBetweenTwoDates(lastOpenDate, incrDate)

      val segment = {

        if (0 <= time4mToday && time4mToday <= 15) {
          15
        } else if (15 < time4mToday && time4mToday <= 30) {
          30
        } else if (30 < time4mToday && time4mToday <= 60) {
          60
        } else if (60 < time4mToday && time4mToday <= 120) {
          120
        } else if (120 < time4mToday && time4mToday <= 180) {
          180
        } else if (180 < time4mToday && time4mToday <= 210) {
          210
        } else if (210 < time4mToday && time4mToday <= 240) {
          240
        } else if (240 < time4mToday && time4mToday <= 270) {
          270
        } else if (270 < time4mToday && time4mToday <= 300) {
          300
        } else if (300 < time4mToday && time4mToday <= 330) {
          330
        } else if (330 < time4mToday && time4mToday <= 360) {
          360
        } else if (time4mToday > 360) {
          "NO"
        }
      }
      segment.toString
    }
  }

  val opens = (opens: Integer, clicks: Integer) => {
    if (opens == null) {
      clicks
    } else {
      opens
    }

  }

  val openDate = (opens: String, clickDate: String) => {
    if (opens == null) {
      clickDate
    } else {
      opens
    }
  }

  val findOpen = udf(opens)

  val findOpenDate = udf(openDate)

  def merge(resultSet: DataFrame, dfCmrFull: DataFrame, nlSubscribers: DataFrame) = {

    /*      resultSet.printSchema	
      val x = resultSet.select(ContactListMobileVars.UID)
      println(x.count)
      println(x.distinct.count)

      dfCmrFull.printSchema
      val y = dfCmrFull.select(ContactListMobileVars.UID)
      println(y.count)
      println(y.distinct.count)    
  */

    val cmrResDf = dfCmrFull.join(resultSet, dfCmrFull(ContactListMobileVars.UID) === resultSet(ContactListMobileVars.UID),
      SQL.LEFT_OUTER).select(
        dfCmrFull(ContactListMobileVars.UID),
        dfCmrFull(CustomerVariables.EMAIL),
        resultSet(EmailResponseVariables.OPEN_7DAYS),
        resultSet(EmailResponseVariables.OPEN_15DAYS),
        resultSet(EmailResponseVariables.OPEN_30DAYS),
        resultSet(EmailResponseVariables.CLICK_7DAYS),
        resultSet(EmailResponseVariables.CLICK_15DAYS),
        resultSet(EmailResponseVariables.CLICK_30DAYS),
        resultSet(EmailResponseVariables.LAST_OPEN_DATE),
        resultSet(EmailResponseVariables.LAST_CLICK_DATE),
        resultSet(EmailResponseVariables.OPENS_LIFETIME),
        resultSet(EmailResponseVariables.CLICKS_LIFETIME),
        resultSet(EmailResponseVariables.END_DATE),
        resultSet(NewsletterVariables.UPDATED_AT)
      )

    val result = cmrResDf.join(nlSubscribers, cmrResDf(CustomerVariables.EMAIL) === nlSubscribers(CustomerVariables.EMAIL),
      SQL.LEFT_OUTER).select(
        cmrResDf(ContactListMobileVars.UID),
        cmrResDf(EmailResponseVariables.OPEN_7DAYS),
        cmrResDf(EmailResponseVariables.OPEN_15DAYS),
        cmrResDf(EmailResponseVariables.OPEN_30DAYS),
        cmrResDf(EmailResponseVariables.CLICK_7DAYS),
        cmrResDf(EmailResponseVariables.CLICK_15DAYS),
        cmrResDf(EmailResponseVariables.CLICK_30DAYS),
        cmrResDf(EmailResponseVariables.LAST_OPEN_DATE),
        cmrResDf(EmailResponseVariables.LAST_CLICK_DATE),
        cmrResDf(EmailResponseVariables.OPENS_LIFETIME),
        cmrResDf(EmailResponseVariables.CLICKS_LIFETIME),
        when(nlSubscribers(NewsletterVariables.STATUS).equalTo("Unsubscribed"), nlSubscribers(NewsletterVariables.UPDATED_AT).cast(StringType))
          .otherwise(cmrResDf(EmailResponseVariables.END_DATE)) as EmailResponseVariables.END_DATE,
        when(nlSubscribers(NewsletterVariables.UPDATED_AT).isNotNull, nlSubscribers(NewsletterVariables.UPDATED_AT).cast(StringType))
          .otherwise(cmrResDf(NewsletterVariables.UPDATED_AT)) as NewsletterVariables.UPDATED_AT)

    result
  }

  def effectiveDFFull(incremental: DataFrame, full: DataFrame, effective7: DataFrame, effective15: DataFrame, effective30: DataFrame): DataFrame = {

    if (incremental == null) {
      logger.error("Incremental DataFrame is null, returning full")
      full
    }

    val joined_7_15 = MergeUtils.joinOldAndNewDF(effective15, CustEmailSchema.reqCsvDf,
      effective7, CustEmailSchema.reqCsvDf, ContactListMobileVars.UID)

    val joined_7_15_summary = joined_7_15.select(
      coalesce(col(MergeUtils.NEW_ + ContactListMobileVars.UID), col(ContactListMobileVars.UID)) as ContactListMobileVars.UID,
      col(EmailResponseVariables.CLICKS_TODAY) as EmailResponseVariables.CLICK_7DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_TODAY) as EmailResponseVariables.CLICK_15DAYS,
      col(EmailResponseVariables.OPENS_TODAY) as EmailResponseVariables.OPEN_7DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.OPENS_TODAY) as EmailResponseVariables.OPEN_15DAYS)
      .withColumn(EmailResponseVariables.OPEN_7DAYS, findOpen(col(EmailResponseVariables.OPEN_7DAYS), col(EmailResponseVariables.CLICK_7DAYS)))
      .withColumn(EmailResponseVariables.OPEN_15DAYS, findOpen(col(EmailResponseVariables.OPEN_15DAYS), col(EmailResponseVariables.CLICK_15DAYS)))

    val joined_7_15_30 = MergeUtils.joinOldAndNewDF(effective30, CustEmailSchema.reqCsvDf,
      joined_7_15_summary, CustEmailSchema.effective7_15Schema, ContactListMobileVars.UID)
      .na.fill(Map(
        EmailResponseVariables.CLICK_15DAYS -> 0,
        EmailResponseVariables.OPEN_15DAYS -> 0,
        EmailResponseVariables.CLICK_7DAYS -> 0,
        EmailResponseVariables.OPEN_7DAYS -> 0,
        MergeUtils.NEW_ + EmailResponseVariables.CLICKS_TODAY -> 0,
        MergeUtils.NEW_ + EmailResponseVariables.OPENS_TODAY -> 0))

    val joined_7_15_30_summary = joined_7_15_30.select(
      coalesce(col(MergeUtils.NEW_ + ContactListMobileVars.UID), col(ContactListMobileVars.UID))
        as ContactListMobileVars.UID,
      col(EmailResponseVariables.CLICK_7DAYS) as EmailResponseVariables.CLICK_7DAYS,
      col(EmailResponseVariables.CLICK_15DAYS) as EmailResponseVariables.CLICK_15DAYS,
      col(EmailResponseVariables.OPEN_7DAYS) as EmailResponseVariables.OPEN_7DAYS,
      col(EmailResponseVariables.OPEN_15DAYS) as EmailResponseVariables.OPEN_15DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_TODAY) as EmailResponseVariables.CLICK_30DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.OPENS_TODAY) as EmailResponseVariables.OPEN_30DAYS)
      .withColumn(EmailResponseVariables.OPEN_30DAYS, findOpen(col(EmailResponseVariables.OPEN_30DAYS), col(EmailResponseVariables.CLICK_30DAYS)))

    val joinedIncr = MergeUtils.joinOldAndNewDF(incremental, CustEmailSchema.resCustomerEmail,
      joined_7_15_30_summary, CustEmailSchema.resCustomerEmail, ContactListMobileVars.UID)
      .na.fill(Map(
        EmailResponseVariables.CLICK_15DAYS -> 0,
        EmailResponseVariables.OPEN_15DAYS -> 0,
        EmailResponseVariables.CLICK_7DAYS -> 0,
        EmailResponseVariables.OPEN_7DAYS -> 0,
        EmailResponseVariables.CLICK_30DAYS -> 0,
        EmailResponseVariables.OPEN_30DAYS -> 0,
        MergeUtils.NEW_ + EmailResponseVariables.CLICKS_TODAY -> 0,
        MergeUtils.NEW_ + EmailResponseVariables.OPENS_TODAY -> 0))

    val joinedIncrSummary = joinedIncr.select(
      coalesce(col(MergeUtils.NEW_ + ContactListMobileVars.UID), col(ContactListMobileVars.UID)) as ContactListMobileVars.UID,
      col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_TODAY) - col(EmailResponseVariables.CLICK_7DAYS) as EmailResponseVariables.CLICK_7DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.OPENS_TODAY) - col(EmailResponseVariables.OPEN_7DAYS) as EmailResponseVariables.OPEN_7DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_TODAY) - col(EmailResponseVariables.CLICK_15DAYS) as EmailResponseVariables.CLICK_15DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.OPENS_TODAY) - col(EmailResponseVariables.OPEN_15DAYS) as EmailResponseVariables.OPEN_15DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_TODAY) - col(EmailResponseVariables.CLICK_30DAYS) as EmailResponseVariables.CLICK_30DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.OPENS_TODAY) - col(EmailResponseVariables.OPEN_30DAYS) as EmailResponseVariables.OPEN_30DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_TODAY) as EmailResponseVariables.CLICKS_LIFETIME,
      col(MergeUtils.NEW_ + EmailResponseVariables.OPENS_TODAY) as EmailResponseVariables.OPENS_LIFETIME,
      col(MergeUtils.NEW_ + EmailResponseVariables.LAST_CLICK_DATE) as EmailResponseVariables.LAST_CLICK_DATE,
      col(MergeUtils.NEW_ + EmailResponseVariables.LAST_OPEN_DATE) as EmailResponseVariables.LAST_OPEN_DATE)

    val prevEffDf = SchemaUtils.addColumns(full, CustEmailSchema.effective_Smry_Schema)

    val incrDateFullDf = MergeUtils.joinOldAndNewDF(joinedIncrSummary, CustEmailSchema.effective_Smry_Schema, prevEffDf,
      CustEmailSchema.effective_Smry_Schema, ContactListMobileVars.UID)
      .na.fill(
        Map(
          EmailResponseVariables.CLICKS_LIFETIME -> 0,
          EmailResponseVariables.OPENS_LIFETIME -> 0,
          EmailResponseVariables.CLICK_7DAYS -> 0,
          EmailResponseVariables.CLICK_15DAYS -> 0,
          EmailResponseVariables.CLICK_30DAYS -> 0,
          EmailResponseVariables.OPEN_7DAYS -> 0,
          EmailResponseVariables.OPEN_15DAYS -> 0,
          EmailResponseVariables.OPEN_30DAYS -> 0,
          MergeUtils.NEW_ + EmailResponseVariables.CLICKS_LIFETIME -> 0,
          MergeUtils.NEW_ + EmailResponseVariables.OPENS_LIFETIME -> 0,
          MergeUtils.NEW_ + EmailResponseVariables.CLICK_7DAYS -> 0,
          MergeUtils.NEW_ + EmailResponseVariables.CLICK_15DAYS -> 0,
          MergeUtils.NEW_ + EmailResponseVariables.CLICK_30DAYS -> 0,
          MergeUtils.NEW_ + EmailResponseVariables.OPEN_7DAYS -> 0,
          MergeUtils.NEW_ + EmailResponseVariables.OPEN_15DAYS -> 0,
          MergeUtils.NEW_ + EmailResponseVariables.OPEN_30DAYS -> 0))

    val incrDatefullSummary = incrDateFullDf.select(
      coalesce(col(ContactListMobileVars.UID), col(MergeUtils.NEW_ + ContactListMobileVars.UID)) as ContactListMobileVars.UID,
      Udf.getLatestEmailOpenDate(lit(""), lit(""), col(MergeUtils.NEW_ + EmailResponseVariables.LAST_CLICK_DATE),
        col(EmailResponseVariables.LAST_CLICK_DATE)) as EmailResponseVariables.LAST_CLICK_DATE,
      Udf.getLatestEmailOpenDate(col(MergeUtils.NEW_ + EmailResponseVariables.LAST_OPEN_DATE), col(EmailResponseVariables.LAST_OPEN_DATE),
        col(MergeUtils.NEW_ + EmailResponseVariables.LAST_CLICK_DATE), col(EmailResponseVariables.LAST_CLICK_DATE))
        as EmailResponseVariables.LAST_OPEN_DATE,
      col(EmailResponseVariables.CLICK_7DAYS) + col(MergeUtils.NEW_ + EmailResponseVariables.CLICK_7DAYS) as EmailResponseVariables.CLICK_7DAYS,
      col(EmailResponseVariables.CLICK_15DAYS) + col(MergeUtils.NEW_ + EmailResponseVariables.CLICK_15DAYS) as EmailResponseVariables.CLICK_15DAYS,
      col(EmailResponseVariables.CLICK_30DAYS) + col(MergeUtils.NEW_ + EmailResponseVariables.CLICK_30DAYS) as EmailResponseVariables.CLICK_30DAYS,
      col(EmailResponseVariables.OPEN_7DAYS) + col(MergeUtils.NEW_ + EmailResponseVariables.OPEN_7DAYS) as EmailResponseVariables.OPEN_7DAYS,
      col(EmailResponseVariables.OPEN_15DAYS) + col(MergeUtils.NEW_ + EmailResponseVariables.OPEN_15DAYS) as EmailResponseVariables.OPEN_15DAYS,
      col(EmailResponseVariables.OPEN_30DAYS) + col(MergeUtils.NEW_ + EmailResponseVariables.OPEN_30DAYS) as EmailResponseVariables.OPEN_30DAYS,
      col(MergeUtils.NEW_ + EmailResponseVariables.CLICKS_LIFETIME).cast(IntegerType) + col(EmailResponseVariables.CLICKS_LIFETIME).cast(IntegerType) as EmailResponseVariables.CLICKS_LIFETIME,
      col(MergeUtils.NEW_ + EmailResponseVariables.OPENS_LIFETIME) + col(EmailResponseVariables.OPENS_LIFETIME) as EmailResponseVariables.OPENS_LIFETIME,
      col(NewsletterVariables.UPDATED_AT) as NewsletterVariables.UPDATED_AT,
      col(EmailResponseVariables.END_DATE) as EmailResponseVariables.END_DATE)
    incrDatefullSummary
  }

  def reduce(df: DataFrame, evenDateType: String, eventNumType: String): DataFrame = {
    df.groupBy(EmailResponseVariables.CUSTOMER_ID)
      .agg(max(EmailResponseVariables.EVENT_CAPTURED_DT) as evenDateType,
        count(EmailResponseVariables.CUSTOMER_ID) as eventNumType)
  }

}
