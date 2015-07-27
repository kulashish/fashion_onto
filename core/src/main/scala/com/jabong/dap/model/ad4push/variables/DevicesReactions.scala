package com.jabong.dap.model.ad4push.variables

import java.text.SimpleDateFormat
import java.util.Calendar
import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.constants.variables.DevicesReactionsVariables
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.acq.common.VarInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.MergeUtils
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.ad4push.schema.DevicesReactionsSchema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import grizzled.slf4j.Logging
import com.jabong.dap.common.udf.Udf

/**
 * Created by Kapil.Rajak on 13/7/15.
 */
object DevicesReactions extends Logging {

  def start(vars: VarInfo) = {
    val incrDate = OptionUtils.getOptValue(vars.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT))
    val mode = OptionUtils.getOptValue(vars.incrMode, DataSets.DAILY_MODE)
    customerResponse(incrDate, mode)
  }
  /**
   * All read CSV, read perquet, write perquet
   * @param incrDate date for which summary is needed in YYYYMMDD format
   * @return (iPhoneResult, AndroidResult) for tgiven date
   */
  def customerResponse(incrDate: String, mode: String): Unit = {

    //getting file names

    val dateStr = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT, TimeConstants.DATE_FORMAT_FOLDER)

    val incIStringSchema = DataReader.getDataFrame4mCsv(DataSets.INPUT_PATH, DataSets.AD4PUSH, DataSets.REACTION_IOS, mode, dateStr, "true", ",")
    val incI = dfCorrectSchema(incIStringSchema)

    val incAStringSchema = DataReader.getDataFrame4mCsv(DataSets.INPUT_PATH, DataSets.AD4PUSH, DataSets.REACTION_ANDROID, mode, dateStr, "true", ",")
    val incA = dfCorrectSchema(incAStringSchema)

    val before7daysString = TimeUtils.getDateAfterNDays(-8, TimeConstants.DATE_FORMAT_FOLDER, dateStr)

    val before15daysString = TimeUtils.getDateAfterNDays(-16, TimeConstants.DATE_FORMAT_FOLDER, dateStr)

    val before30daysString = TimeUtils.getDateAfterNDays(-31, TimeConstants.DATE_FORMAT_FOLDER, dateStr)

    val yesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER, dateStr)

    //getting DF
    logger.info("Reading inputs (CSVs and Parquets)")
    val fullI = DataReader.getDataFrame(DataSets.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTION_IOS, DataSets.FULL, yesterday)
    val b7I = DataReader.getDataFrame(DataSets.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTION_IOS, DataSets.DAILY_MODE, before7daysString)
    val b15I = DataReader.getDataFrame(DataSets.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTION_IOS, DataSets.DAILY_MODE, before15daysString)
    val b30I = DataReader.getDataFrame(DataSets.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTION_IOS, DataSets.DAILY_MODE, before30daysString)

    val fullA = DataReader.getDataFrame(DataSets.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTION_ANDROID, DataSets.FULL, yesterday)
    val b7A = DataReader.getDataFrame(DataSets.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTION_ANDROID, DataSets.FULL, before7daysString)
    val b15A = DataReader.getDataFrame(DataSets.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTION_ANDROID, DataSets.FULL, before15daysString)
    val b30A = DataReader.getDataFrame(DataSets.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTION_ANDROID, DataSets.FULL, before30daysString)

    val (resultI, incrI) = fullSummary(incI, dateStr, fullI, b7I, b15I, b30I)
    val (resultA, incrA) = fullSummary(incA, dateStr, fullA, b7A, b15A, b30A)

    DataWriter.writeParquet(resultI, DataSets.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTION_IOS, DataSets.FULL, dateStr)
    DataWriter.writeParquet(incrI, DataSets.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTION_IOS, mode, dateStr)

    DataWriter.writeParquet(resultA, DataSets.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTION_ANDROID, DataSets.FULL, dateStr)
    DataWriter.writeParquet(incrA, DataSets.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTION_ANDROID, mode, dateStr)
  }

  def dfCorrectSchema(df: DataFrame): DataFrame = {
    return df.select(df(DevicesReactionsVariables.LOGIN_USER_ID) as DevicesReactionsVariables.CUSTOMER_ID,
      df(DevicesReactionsVariables.DEVICE_ID) as DevicesReactionsVariables.DEVICE_ID,
      df(DevicesReactionsVariables.MESSAGE_ID) as DevicesReactionsVariables.MESSAGE_ID,
      df(DevicesReactionsVariables.CAMPAIGN_ID) as DevicesReactionsVariables.CAMPAIGN_ID,
      df(DevicesReactionsVariables.BOUNCE).cast(IntegerType) as DevicesReactionsVariables.BOUNCE,
      df(DevicesReactionsVariables.REACTION).cast(IntegerType) as DevicesReactionsVariables.REACTION)
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

    val incrDay = TimeUtils.dayName(incrDate, TimeConstants.YYYYMMDD).toLowerCase

    val reducedIncr = reduce(incrementalDF)

    val effective = effectiveDFFull(reducedIncr, reduced7, reduced15, reduced30).withColumnRenamed(DevicesReactionsVariables.CUSTOMER_ID, DevicesReactionsVariables.CUSTOMER_ID)

    val joinedDF = MergeUtils.joinOldAndNewDF(effective, DevicesReactionsSchema.effectiveDF, full, DevicesReactionsSchema.deviceReaction, DevicesReactionsVariables.DEVICE_ID, DevicesReactionsVariables.CUSTOMER_ID)

    val resultDF = joinedDF.select(
      coalesce(col(MergeUtils.NEW_ + DevicesReactionsVariables.DEVICE_ID), col(DevicesReactionsVariables.DEVICE_ID)) as DevicesReactionsVariables.DEVICE_ID,
      coalesce(col(MergeUtils.NEW_ + DevicesReactionsVariables.CUSTOMER_ID), col(DevicesReactionsVariables.CUSTOMER_ID)) as DevicesReactionsVariables.CUSTOMER_ID,
      when(col(MergeUtils.NEW_ + DevicesReactionsVariables.CLICKED_TODAY) > 0, TimeUtils.changeDateFormat(incrDate, TimeConstants.YYYYMMDD, TimeConstants.DATE_FORMAT)).otherwise(col(DevicesReactionsVariables.LAST_CLICK_DATE)) as DevicesReactionsVariables.LAST_CLICK_DATE,
      (col(DevicesReactionsVariables.CLICK_7) + col(MergeUtils.NEW_ + DevicesReactionsVariables.EFFECTIVE_7_DAYS)).cast(IntegerType) as DevicesReactionsVariables.CLICK_7,
      (col(DevicesReactionsVariables.CLICK_15) + col(MergeUtils.NEW_ + DevicesReactionsVariables.EFFECTIVE_15_DAYS)).cast(IntegerType) as DevicesReactionsVariables.CLICK_15,
      (col(DevicesReactionsVariables.CLICK_30) + col(MergeUtils.NEW_ + DevicesReactionsVariables.EFFECTIVE_30_DAYS)).cast(IntegerType) as DevicesReactionsVariables.CLICK_30,
      (col(DevicesReactionsVariables.CLICK_LIFETIME) + col(MergeUtils.NEW_ + DevicesReactionsVariables.CLICKED_TODAY)).cast(IntegerType) as DevicesReactionsVariables.CLICK_LIFETIME,
      (when(col(MergeUtils.NEW_ + DevicesReactionsVariables.CLICKED_TODAY) > 2, col(DevicesReactionsVariables.CLICKED_TWICE) + 1).otherwise(col(DevicesReactionsVariables.CLICKED_TWICE))).cast(IntegerType) as DevicesReactionsVariables.CLICKED_TWICE,

      (col(DevicesReactionsVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 0).toLowerCase()) + col(MergeUtils.NEW_ + DevicesReactionsVariables.CLICKED_TODAY)).cast(IntegerType) as DevicesReactionsVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 0).toLowerCase(),
      (col(DevicesReactionsVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 1).toLowerCase())).cast(IntegerType) as DevicesReactionsVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 1).toLowerCase(),
      (col(DevicesReactionsVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 2).toLowerCase())).cast(IntegerType) as DevicesReactionsVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 2).toLowerCase(),
      (col(DevicesReactionsVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 3).toLowerCase())).cast(IntegerType) as DevicesReactionsVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 3).toLowerCase(),
      (col(DevicesReactionsVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 4).toLowerCase())).cast(IntegerType) as DevicesReactionsVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 4).toLowerCase(),
      (col(DevicesReactionsVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 5).toLowerCase())).cast(IntegerType) as DevicesReactionsVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 5).toLowerCase(),
      (col(DevicesReactionsVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 6).toLowerCase())).cast(IntegerType) as DevicesReactionsVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 6).toLowerCase()
    )

    val result = resultDF.select(col(DevicesReactionsVariables.DEVICE_ID), col(DevicesReactionsVariables.CUSTOMER_ID), col(DevicesReactionsVariables.LAST_CLICK_DATE), col(DevicesReactionsVariables.CLICK_7), col(DevicesReactionsVariables.CLICK_15), col(DevicesReactionsVariables.CLICK_30), col(DevicesReactionsVariables.CLICK_LIFETIME),
      col(DevicesReactionsVariables.CLICK_MONDAY), col(DevicesReactionsVariables.CLICK_TUESDAY), col(DevicesReactionsVariables.CLICK_WEDNESDAY), col(DevicesReactionsVariables.CLICK_THURSDAY),
      col(DevicesReactionsVariables.CLICK_FRIDAY), col(DevicesReactionsVariables.CLICK_SATURDAY), col(DevicesReactionsVariables.CLICK_SUNDAY), col(DevicesReactionsVariables.CLICKED_TWICE),
      Udf.maxClickDayName(col(DevicesReactionsVariables.CLICK_MONDAY), col(DevicesReactionsVariables.CLICK_TUESDAY), col(DevicesReactionsVariables.CLICK_WEDNESDAY), col(DevicesReactionsVariables.CLICK_THURSDAY), col(DevicesReactionsVariables.CLICK_FRIDAY), col(DevicesReactionsVariables.CLICK_SATURDAY), col(DevicesReactionsVariables.CLICK_SUNDAY)) as DevicesReactionsVariables.MOST_CLICK_DAY
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

    val joined_7_15 = MergeUtils.joinOldAndNewDF(effective15, DevicesReactionsSchema.reducedDF, effective7, DevicesReactionsSchema.reducedDF, DevicesReactionsVariables.DEVICE_ID, DevicesReactionsVariables.CUSTOMER_ID)
    val joined_7_15_summary = joined_7_15.select(
      coalesce(col(DevicesReactionsVariables.DEVICE_ID), col(MergeUtils.NEW_ + DevicesReactionsVariables.DEVICE_ID)) as DevicesReactionsVariables.DEVICE_ID,
      coalesce(col(DevicesReactionsVariables.CUSTOMER_ID), col(DevicesReactionsVariables.CUSTOMER_ID)) as DevicesReactionsVariables.CUSTOMER_ID,
      col(DevicesReactionsVariables.REACTION) as DevicesReactionsVariables.EFFECTIVE_7_DAYS,
      col(MergeUtils.NEW_ + DevicesReactionsVariables.REACTION) as DevicesReactionsVariables.EFFECTIVE_15_DAYS)
      .na.fill(0)

    val joined_7_15_30 = MergeUtils.joinOldAndNewDF(effective30, DevicesReactionsSchema.reducedDF, joined_7_15_summary, DevicesReactionsSchema.joined_7_15, DevicesReactionsVariables.DEVICE_ID, DevicesReactionsVariables.CUSTOMER_ID)
    val joined_7_15_30_summary = joined_7_15_30.select(
      coalesce(col(MergeUtils.NEW_ + DevicesReactionsVariables.DEVICE_ID), col(DevicesReactionsVariables.DEVICE_ID)) as DevicesReactionsVariables.DEVICE_ID,
      coalesce(col(MergeUtils.NEW_ + DevicesReactionsVariables.CUSTOMER_ID), col(DevicesReactionsVariables.CUSTOMER_ID)) as DevicesReactionsVariables.CUSTOMER_ID,
      col(DevicesReactionsVariables.EFFECTIVE_7_DAYS) as DevicesReactionsVariables.EFFECTIVE_7_DAYS,
      col(DevicesReactionsVariables.EFFECTIVE_15_DAYS) as DevicesReactionsVariables.EFFECTIVE_15_DAYS,
      col(MergeUtils.NEW_ + DevicesReactionsVariables.REACTION) as DevicesReactionsVariables.EFFECTIVE_30_DAYS)
      .na.fill(0)

    val joinedAll = MergeUtils.joinOldAndNewDF(incremental, DevicesReactionsSchema.reducedDF, joined_7_15_30_summary, DevicesReactionsSchema.joined_7_15_30, DevicesReactionsVariables.DEVICE_ID, DevicesReactionsVariables.CUSTOMER_ID)
    val joinedAllSummary = joinedAll.select(
      coalesce(col(MergeUtils.NEW_ + DevicesReactionsVariables.DEVICE_ID), col(DevicesReactionsVariables.DEVICE_ID)) as DevicesReactionsVariables.DEVICE_ID,
      coalesce(col(MergeUtils.NEW_ + DevicesReactionsVariables.CUSTOMER_ID), col(DevicesReactionsVariables.CUSTOMER_ID)) as DevicesReactionsVariables.CUSTOMER_ID,
      col(MergeUtils.NEW_ + DevicesReactionsVariables.REACTION) - col(DevicesReactionsVariables.EFFECTIVE_7_DAYS) as DevicesReactionsVariables.EFFECTIVE_7_DAYS,
      col(MergeUtils.NEW_ + DevicesReactionsVariables.REACTION) - col(DevicesReactionsVariables.EFFECTIVE_15_DAYS) as DevicesReactionsVariables.EFFECTIVE_15_DAYS,
      col(MergeUtils.NEW_ + DevicesReactionsVariables.REACTION) - col(DevicesReactionsVariables.EFFECTIVE_30_DAYS) as DevicesReactionsVariables.EFFECTIVE_30_DAYS,
      col(MergeUtils.NEW_ + DevicesReactionsVariables.REACTION) as DevicesReactionsVariables.CLICKED_TODAY)
      .na.fill(0)

    return joinedAllSummary
  }

  /**
   * @param df DevicesReactions DataFrame with columns(CUSTOMER_ID,DEVICE_ID,REACTION) & other columns are optional
   * @return reduced df with DEVICE_ID as primary key and sum(REACTIONS) evaluated for each key & CUSTOMER_ID taken
   */
  def reduce(df: DataFrame): DataFrame = {
    if (df == null) {
      logger.info("DataFrame df is null, returning null")
      return null
    }
    return df.select(DevicesReactionsVariables.CUSTOMER_ID, DevicesReactionsVariables.DEVICE_ID, DevicesReactionsVariables.REACTION).groupBy(DevicesReactionsVariables.DEVICE_ID, DevicesReactionsVariables.CUSTOMER_ID).agg(sum(DevicesReactionsVariables.REACTION).cast(IntegerType) as DevicesReactionsVariables.REACTION)
      .select(DevicesReactionsVariables.CUSTOMER_ID, DevicesReactionsVariables.DEVICE_ID, DevicesReactionsVariables.REACTION)
  }

}
