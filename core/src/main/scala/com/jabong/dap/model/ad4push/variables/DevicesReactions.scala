package com.jabong.dap.model.ad4push.variables

import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar
import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.variables.DevicesReactionsVariables._
import com.jabong.dap.common.time.{ Constants, TimeUtils }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.MergeUtils
import com.jabong.dap.model.ad4push.schema.DevicesReactionsSchema
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import grizzled.slf4j.Logging
import com.jabong.dap.common.udf.Udf

/**
 * Created by Kapil.Rajak on 13/7/15.
 */
object DevicesReactions extends Logging {
  /**
   * @param source
   * @return DataFrame from the path
   */
  def readParquet(source: String, fileName: String, mode: String, date: String): DataFrame = {
    var df: DataFrame = null
    try {
      df = Spark.getSqlContext().read.parquet(source + File.separator + fileName + File.separator + mode + File.separator + date)
    } catch {
      case ae: AssertionError => logger.error(ae.getMessage)
    }
    return df
  }

  /**
   * @param source
   * @return true if success else false
   */
  def writeDF(df: DataFrame, source: String, fileName: String, mode: String, date: String) {
    df.write.parquet(source + File.separator + fileName + File.separator + mode + File.separator + fileName)
  }

  /**
   * All read CSV, read perquet, write perquet
   * @param yyyyMMdd date for which summary is needed in YYYYMMDD format
   * @return (iPhoneResult, AndroidResult) for tgiven date
   */
  def customerResponse(yyyyMMdd: String, mode: String) = {

    //getting file names
    val today = Calendar.getInstance().getTime()
    val formatter = new SimpleDateFormat(Constants.YYYYMMDD)

    val dateString = if (yyyyMMdd != null) yyyyMMdd else formatter.format(today)

    val CUSTOMER_RESPONSE_PATH = DataSets.basePath + File.separator + DataSets.CUSTOMER_RESPONSE

    val dateStr = TimeUtils.changeDateFormat(dateString, Constants.YYYYMMDD, Constants.DATE_FORMAT_FOLDER)

    val incIPhoneCSV = DataSets.IPHONE_CSV_PREFIX + dateString + DataSets.CSV_EXTENSION
    val incI = readCsv(CUSTOMER_RESPONSE_PATH, mode, dateStr, incIPhoneCSV)

    val incAndroidCSV = DataSets.ANDROID_CSV_PREFIX + dateString + DataSets.CSV_EXTENSION
    val incA = readCsv(CUSTOMER_RESPONSE_PATH, mode, dateStr, incAndroidCSV)

    val before7daysString = TimeUtils.getDateAfterNDays(-8, Constants.DATE_FORMAT_FOLDER, dateStr)

    val before15daysString = TimeUtils.getDateAfterNDays(-16, Constants.DATE_FORMAT_FOLDER, dateStr)

    val before30daysString = TimeUtils.getDateAfterNDays(-31, Constants.DATE_FORMAT_FOLDER, dateStr)

    val yesterday = TimeUtils.getDateAfterNDays(-1, Constants.DATE_FORMAT_FOLDER, dateStr)

    val CUSTOMER_RESPONSE_VAR_PATH = DataSets.VARIABLE_PATH + File.separator + DataSets.CUSTOMER_RESPONSE

    //getting DF
    logger.info("Reading inputs (CSVs and Parquets)")
    val fullI = readParquet(CUSTOMER_RESPONSE_VAR_PATH, DataSets.IPHONE, DataSets.FULL_MODE, yesterday)
    val b7I = readParquet(CUSTOMER_RESPONSE_VAR_PATH, DataSets.IPHONE, DataSets.DAILY_MODE, before7daysString)
    val b15I = readParquet(CUSTOMER_RESPONSE_VAR_PATH, DataSets.IPHONE, DataSets.DAILY_MODE, before15daysString)
    val b30I = readParquet(CUSTOMER_RESPONSE_VAR_PATH, DataSets.IPHONE, DataSets.DAILY_MODE, before30daysString)

    val fullA = readParquet(CUSTOMER_RESPONSE_VAR_PATH, DataSets.ANDROID, DataSets.FULL_MODE, yesterday)
    val b7A = readParquet(CUSTOMER_RESPONSE_VAR_PATH, DataSets.ANDROID, DataSets.FULL_MODE, before7daysString)
    val b15A = readParquet(CUSTOMER_RESPONSE_VAR_PATH, DataSets.ANDROID, DataSets.FULL_MODE, before15daysString)
    val b30A = readParquet(CUSTOMER_RESPONSE_VAR_PATH, DataSets.ANDROID, DataSets.FULL_MODE, before30daysString)

    val (resultI, incrI) = fullSummary(incI, dateStr, fullI, b7I, b15I, b30I)
    val (resultA, incrA) = fullSummary(incA, dateStr, fullA, b7A, b15A, b30A)

    writeDF(resultI, CUSTOMER_RESPONSE_VAR_PATH, DataSets.IPHONE, DataSets.FULL_MODE, dateStr)
    writeDF(incrI, CUSTOMER_RESPONSE_VAR_PATH, DataSets.IPHONE, mode, dateStr)

    writeDF(resultA, CUSTOMER_RESPONSE_VAR_PATH, DataSets.ANDROID, DataSets.FULL_MODE, dateStr)
    writeDF(incrA, CUSTOMER_RESPONSE_VAR_PATH, DataSets.ANDROID, mode, dateStr)

  }

  /**
   *
   * @param incrementalDF
   * @param incrDate Today's date in YYYYMMDD format
   * @param full yesterday's perquet
   * @param reduced7 8day's before DF
   * @param reduced15 16 day's before DF
   * @param reduced30 31 day's before DF
   * @return fullDF
   */
  def fullSummary(incrementalDF: DataFrame, incrDate: String, full: DataFrame, reduced7: DataFrame, reduced15: DataFrame, reduced30: DataFrame): (DataFrame, DataFrame) = {

    logger.info("Processing DeviceReaction for :" + incrDate)

    if (incrementalDF == null) {
      logger.error("Incremental DataFrame is null, returning full")
      return (full, null)
    }

    val incrDay = TimeUtils.dayName(incrDate, Constants.YYYYMMDD).toLowerCase

    val reducedIncr = reduce(incrementalDF)

    val effective = effectiveDFFull(reducedIncr, reduced7, reduced15, reduced30)

    val joinedDF = MergeUtils.joinOldAndNewDF(effective, DevicesReactionsSchema.effectiveDF, full, DevicesReactionsSchema.deviceReaction, DEVICE_ID)

    val resultDF = joinedDF.select(
      coalesce(col(MergeUtils.NEW_ + DEVICE_ID), col(DEVICE_ID)) as DEVICE_ID,
      coalesce(col(MergeUtils.NEW_ + LOGIN_USER_ID), col(CUSTOMER_ID)) as CUSTOMER_ID,
      when(col(MergeUtils.NEW_ + CLICKED_TODAY) > 0, TimeUtils.changeDateFormat(incrDate, Constants.YYYYMMDD, Constants.DATE_FORMAT)).otherwise(col(LAST_CLICK_DATE)) as LAST_CLICK_DATE,
      (col(CLICK_7) + col(MergeUtils.NEW_ + EFFECTIVE_7_DAYS)).cast(IntegerType) as CLICK_7,
      (col(CLICK_15) + col(MergeUtils.NEW_ + EFFECTIVE_15_DAYS)).cast(IntegerType) as CLICK_15,
      (col(CLICK_30) + col(MergeUtils.NEW_ + EFFECTIVE_30_DAYS)).cast(IntegerType) as CLICK_30,
      (col(CLICK_LIFETIME) + col(MergeUtils.NEW_ + CLICKED_TODAY)).cast(IntegerType) as CLICK_LIFETIME,
      (when(col(MergeUtils.NEW_ + CLICKED_TODAY) > 2, col(CLICKED_TWICE) + 1).otherwise(col(CLICKED_TWICE))).cast(IntegerType) as CLICKED_TWICE,

      (col(CLICK_ + TimeUtils.nextNDay(incrDay, 0).toLowerCase()) + col(MergeUtils.NEW_ + CLICKED_TODAY)).cast(IntegerType) as CLICK_ + TimeUtils.nextNDay(incrDay, 0).toLowerCase(),
      (col(CLICK_ + TimeUtils.nextNDay(incrDay, 1).toLowerCase())).cast(IntegerType) as CLICK_ + TimeUtils.nextNDay(incrDay, 1).toLowerCase(),
      (col(CLICK_ + TimeUtils.nextNDay(incrDay, 2).toLowerCase())).cast(IntegerType) as CLICK_ + TimeUtils.nextNDay(incrDay, 2).toLowerCase(),
      (col(CLICK_ + TimeUtils.nextNDay(incrDay, 3).toLowerCase())).cast(IntegerType) as CLICK_ + TimeUtils.nextNDay(incrDay, 3).toLowerCase(),
      (col(CLICK_ + TimeUtils.nextNDay(incrDay, 4).toLowerCase())).cast(IntegerType) as CLICK_ + TimeUtils.nextNDay(incrDay, 4).toLowerCase(),
      (col(CLICK_ + TimeUtils.nextNDay(incrDay, 5).toLowerCase())).cast(IntegerType) as CLICK_ + TimeUtils.nextNDay(incrDay, 5).toLowerCase(),
      (col(CLICK_ + TimeUtils.nextNDay(incrDay, 6).toLowerCase())).cast(IntegerType) as CLICK_ + TimeUtils.nextNDay(incrDay, 6).toLowerCase()
    )

    val result = resultDF.select(col(DEVICE_ID), col(CUSTOMER_ID), col(LAST_CLICK_DATE), col(CLICK_7), col(CLICK_15), col(CLICK_30), col(CLICK_LIFETIME),
      col(CLICK_MONDAY), col(CLICK_TUESDAY), col(CLICK_WEDNESDAY), col(CLICK_THURSDAY),
      col(CLICK_FRIDAY), col(CLICK_SATURDAY), col(CLICK_SUNDAY), col(CLICKED_TWICE),
      Udf.maxClickDayName(col(CLICK_MONDAY), col(CLICK_TUESDAY), col(CLICK_WEDNESDAY), col(CLICK_THURSDAY), col(CLICK_FRIDAY), col(CLICK_SATURDAY), col(CLICK_SUNDAY)) as MOST_CLICK_DAY
    )
    logger.info("DeviceReaction for :" + incrDate + "processed")
    (result, reducedIncr)
  }

  /**
   * effectiveDF is the effective clicks with ( -previous clicks + today's click) for 7, 15, 30 days
   * @param incremental yesterday's data
   * @param effective7 8 days before
   * @param effective15 16 days before
   * @param effective30 30 days before
   * @return return effective DF
   */
  def effectiveDFFull(incremental: DataFrame, effective7: DataFrame, effective15: DataFrame, effective30: DataFrame): DataFrame = {
    //send DataFrames after using reduce

    val joined_7_15 = MergeUtils.joinOldAndNewDF(effective15, DevicesReactionsSchema.reducedDF, effective7, DevicesReactionsSchema.reducedDF, DEVICE_ID)
    val joined_7_15_summary = joined_7_15.select(
      coalesce(col(DEVICE_ID), col(MergeUtils.NEW_ + DEVICE_ID)) as DEVICE_ID,
      coalesce(col(LOGIN_USER_ID), col(LOGIN_USER_ID)) as LOGIN_USER_ID,
      col(REACTION) as EFFECTIVE_7_DAYS,
      col(MergeUtils.NEW_ + REACTION) as EFFECTIVE_15_DAYS)
      .na.fill(0)

    val joined_7_15_30 = MergeUtils.joinOldAndNewDF(effective30, DevicesReactionsSchema.reducedDF, joined_7_15_summary, DevicesReactionsSchema.joined_7_15, DEVICE_ID)
    val joined_7_15_30_summary = joined_7_15_30.select(
      coalesce(col(MergeUtils.NEW_ + DEVICE_ID), col(DEVICE_ID)) as DEVICE_ID,
      coalesce(col(MergeUtils.NEW_ + LOGIN_USER_ID), col(LOGIN_USER_ID)) as LOGIN_USER_ID,
      col(EFFECTIVE_7_DAYS) as EFFECTIVE_7_DAYS,
      col(EFFECTIVE_15_DAYS) as EFFECTIVE_15_DAYS,
      col(MergeUtils.NEW_ + REACTION) as EFFECTIVE_30_DAYS)
      .na.fill(0)

    val joinedAll = MergeUtils.joinOldAndNewDF(incremental, DevicesReactionsSchema.reducedDF, joined_7_15_30_summary, DevicesReactionsSchema.joined_7_15_30, DEVICE_ID)
    val joinedAllSummary = joinedAll.select(
      coalesce(col(MergeUtils.NEW_ + DEVICE_ID), col(DEVICE_ID)) as DEVICE_ID,
      coalesce(col(MergeUtils.NEW_ + LOGIN_USER_ID), col(LOGIN_USER_ID)) as LOGIN_USER_ID,
      col(MergeUtils.NEW_ + REACTION) - col(EFFECTIVE_7_DAYS) as EFFECTIVE_7_DAYS,
      col(MergeUtils.NEW_ + REACTION) - col(EFFECTIVE_15_DAYS) as EFFECTIVE_15_DAYS,
      col(MergeUtils.NEW_ + REACTION) - col(EFFECTIVE_30_DAYS) as EFFECTIVE_30_DAYS,
      col(MergeUtils.NEW_ + REACTION) as CLICKED_TODAY)
      .na.fill(0)

    return joinedAllSummary
  }

  /**
   * @param df DevicesReactions DataFrame with columns(LOGIN_USER_ID,DEVICE_ID,REACTION) & other columns are optional
   * @return reduced df with DEVICE_ID as primary key and sum(REACTIONS) evaluated for each key & LOGIN_USER_ID taken
   */
  def reduce(df: DataFrame): DataFrame = {
    if (df == null) {
      logger.info("DataFrame df is null, returning null")
      return null
    }
    return df.select(LOGIN_USER_ID, DEVICE_ID, REACTION).groupBy(DEVICE_ID, LOGIN_USER_ID).agg(max(LOGIN_USER_ID) as LOGIN_USER_ID, sum(REACTION).cast(IntegerType) as REACTION)
      .select(LOGIN_USER_ID, DEVICE_ID, REACTION)
  }

  /**
   * @param path
   * @return df for the CSV with given path
   */
  def readCsv(path: String, mode: String, date: String, filename: String): DataFrame = {
    var df: DataFrame = null
    try {
      df = Spark.getSqlContext().read.format("com.databricks.spark.csv").option("header", "true").load(path + File.separator + mode + File.separator + date + File.separator + filename)
    } catch {
      case iie: InvalidInputException =>
        logger.info(iie.getMessage)
        return null
    }
    df.select(col(LOGIN_USER_ID), col(DEVICE_ID), col(MESSAGE_ID), col(CAMPAIGN_ID), col(BOUNCE).cast(IntegerType) as BOUNCE, col(REACTION).cast(IntegerType) as REACTION)
  }

}
