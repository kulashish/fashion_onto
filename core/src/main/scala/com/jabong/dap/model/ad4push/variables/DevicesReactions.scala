package com.jabong.dap.model.ad4push.variables

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.Ad4pushVariables
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.MergeUtils
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.ad4push.schema.Ad4pushSchema
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
 * Created by Kapil.Rajak on 13/7/15.
 */
object DevicesReactions extends Logging {

  val new_reaction = MergeUtils.NEW_ + Ad4pushVariables.REACTION

  def start(params: ParamInfo) = {
    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val saveMode = params.saveMode
    customerResponse(incrDate, saveMode)
  }
  /**
   * All read CSV, read perquet, write perquet
   * @param incrDate date for which summary is needed in YYYY/MM/DD format
   * @return (iPhoneResult, AndroidResult) for tgiven date
   */
  def customerResponse(incrDate: String, saveMode: String) = {

    val before7daysString = TimeUtils.getDateAfterNDays(-7, TimeConstants.DATE_FORMAT_FOLDER, incrDate)

    val before15daysString = TimeUtils.getDateAfterNDays(-15, TimeConstants.DATE_FORMAT_FOLDER, incrDate)

    val before30daysString = TimeUtils.getDateAfterNDays(-30, TimeConstants.DATE_FORMAT_FOLDER, incrDate)

    val yesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER, incrDate)

    val incrDateInFileFormat = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)

    val savePathI = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_IOS, DataSets.FULL_MERGE_MODE, incrDate)

    var unionFinalDF: DataFrame = null

    if (DataWriter.canWrite(savePathI, saveMode)) {
      val fName = "exportMessagesReactions_" + DataSets.IOS_CODE + "_" + incrDateInFileFormat + ".csv"
      val incIStringSchema = DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_IOS, DataSets.DAILY_MODE, incrDate, fName, "true", ",")
      val incI = dfCorrectSchema(incIStringSchema)

      //getting DF
      logger.info("Reading inputs (CSVs and Parquets) for IOS")
      val fullI = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_IOS, DataSets.FULL_MERGE_MODE, yesterday)
      val b7I = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_IOS, DataSets.DAILY_MODE, before7daysString)
      val b15I = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_IOS, DataSets.DAILY_MODE, before15daysString)
      val b30I = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_IOS, DataSets.DAILY_MODE, before30daysString)

      val (resultI, incrI) = fullSummary(incI, incrDate, fullI, b7I, b15I, b30I)

      val incrSavePathI = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_IOS, DataSets.DAILY_MODE, incrDate)
      if (DataWriter.canWrite(incrSavePathI, saveMode)) {
        DataWriter.writeParquet(incrI, incrSavePathI, saveMode)
      }

      DataWriter.writeParquet(resultI, savePathI, saveMode)

      unionFinalDF = resultI

      //      val filename = DataSets.AD4PUSH + "_" + DataSets.CUSTOMER_RESPONSE + "_" + DataSets.IOS + "_" + incrDateInFileFormat
      //
      //      DataWriter.writeCsv(resultI, DataSets.AD4PUSH, DataSets.REACTIONS_IOS_CSV, DataSets.FULL_MERGE_MODE, incrDate, filename, "true", ",")
    }

    val savePathA = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_ANDROID, DataSets.FULL_MERGE_MODE, incrDate)
    if (DataWriter.canWrite(savePathA, saveMode)) {
      val fName = "exportMessagesReactions_" + DataSets.ANDROID_CODE + "_" + incrDateInFileFormat + ".csv"
      val incAStringSchema = DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_ANDROID, DataSets.DAILY_MODE, incrDate, fName, "true", ",")
      val incA = dfCorrectSchema(incAStringSchema)

      //getting DF
      logger.info("Reading inputs (CSVs and Parquets) for Android")
      val fullA = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_ANDROID, DataSets.FULL_MERGE_MODE, yesterday)
      val b7A = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_ANDROID, DataSets.DAILY_MODE, before7daysString)
      val b15A = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_ANDROID, DataSets.DAILY_MODE, before15daysString)
      val b30A = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_ANDROID, DataSets.DAILY_MODE, before30daysString)

      val (resultA, incrA) = fullSummary(incA, incrDate, fullA, b7A, b15A, b30A)

      val incrSavePathA = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_ANDROID, DataSets.DAILY_MODE, incrDate)
      if (DataWriter.canWrite(incrSavePathA, saveMode)) {
        DataWriter.writeParquet(incrA, incrSavePathA, saveMode)
      }

      DataWriter.writeParquet(resultA, savePathA, saveMode)

      unionFinalDF = unionFinalDF.unionAll(resultA)

      //      val filename = DataSets.AD4PUSH + "_" + DataSets.CUSTOMER_RESPONSE + "_" + DataSets.ANDROID + "_" + incrDateInFileFormat
      //      DataWriter.writeCsv(resultA, DataSets.AD4PUSH, DataSets.REACTIONS_ANDROID_CSV, DataSets.FULL_MERGE_MODE, incrDate, filename, "true", ",")
    }

    val filename = DataSets.AD4PUSH + "_" + DataSets.CUSTOMER_RESPONSE + "_" + incrDateInFileFormat
    DataWriter.writeCsv(unionFinalDF, DataSets.AD4PUSH, DataSets.CUSTOMER_RESPONSE, DataSets.FULL, incrDate, filename, saveMode, "true", ",")
  }

  def dfCorrectSchema(df: DataFrame): DataFrame = {
    df.select(Udf.removeAllZero(df(Ad4pushVariables.LOGIN_USER_ID)) as Ad4pushVariables.CUSTOMER_ID,
      Udf.removeAllZero(df(Ad4pushVariables.DEVICE_ID)) as Ad4pushVariables.DEVICE_ID,
      df(Ad4pushVariables.MESSAGE_ID) as Ad4pushVariables.MESSAGE_ID,
      df(Ad4pushVariables.CAMPAIGN_ID) as Ad4pushVariables.CAMPAIGN_ID,
      df(Ad4pushVariables.BOUNCE).cast(IntegerType) as Ad4pushVariables.BOUNCE,
      df(Ad4pushVariables.REACTION).cast(IntegerType) as Ad4pushVariables.REACTION)
      .filter(!(col(Ad4pushVariables.CUSTOMER_ID) === "" && col(Ad4pushVariables.DEVICE_ID) === ""))
      .filter(col(Ad4pushVariables.REACTION).gt(0) || col(Ad4pushVariables.BOUNCE).gt(0))
  }
  /**
   *
   * @param incrementalDF
   * @param incrDate Today's date in YYYY/MM/DD format
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

    val incrDay = TimeUtils.dayName(incrDate, TimeConstants.DATE_FORMAT_FOLDER).toLowerCase

    val reducedIncr = reduce(incrementalDF).cache()

    val effective = effectiveDFFull(reducedIncr, reduced7, reduced15, reduced30).withColumnRenamed(Ad4pushVariables.CUSTOMER_ID, Ad4pushVariables.CUSTOMER_ID)

    val joinedDF = MergeUtils.joinOldAndNewDF(effective, Ad4pushSchema.effectiveDF, full,
      Ad4pushSchema.deviceReaction, Ad4pushVariables.DEVICE_ID, Ad4pushVariables.CUSTOMER_ID)
      .na.fill(
        Map(
          Ad4pushVariables.CLICK_7 -> 0,
          Ad4pushVariables.CLICK_15 -> 0,
          Ad4pushVariables.CLICK_30 -> 0,
          Ad4pushVariables.CLICK_LIFETIME -> 0,
          Ad4pushVariables.CLICKED_TWICE -> 0,
          Ad4pushVariables.CLICK_MONDAY -> 0,
          Ad4pushVariables.CLICK_TUESDAY -> 0,
          Ad4pushVariables.CLICK_WEDNESDAY -> 0,
          Ad4pushVariables.CLICK_THURSDAY -> 0,
          Ad4pushVariables.CLICK_FRIDAY -> 0,
          Ad4pushVariables.CLICK_SATURDAY -> 0,
          Ad4pushVariables.CLICK_SUNDAY -> 0,
          MergeUtils.NEW_ + Ad4pushVariables.EFFECTIVE_7_DAYS -> 0,
          MergeUtils.NEW_ + Ad4pushVariables.EFFECTIVE_15_DAYS -> 0,
          MergeUtils.NEW_ + Ad4pushVariables.EFFECTIVE_30_DAYS -> 0,
          MergeUtils.NEW_ + Ad4pushVariables.CLICKED_TODAY -> 0
        )
      )

    val resultDF = joinedDF.select(
      coalesce(col(MergeUtils.NEW_ + Ad4pushVariables.DEVICE_ID), col(Ad4pushVariables.DEVICE_ID)) as Ad4pushVariables.DEVICE_ID,
      coalesce(col(MergeUtils.NEW_ + Ad4pushVariables.CUSTOMER_ID), col(Ad4pushVariables.CUSTOMER_ID)) as Ad4pushVariables.CUSTOMER_ID,
      when(col(MergeUtils.NEW_ + Ad4pushVariables.CLICKED_TODAY) > 0, TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_FORMAT)).otherwise(col(Ad4pushVariables.LAST_CLICK_DATE)) as Ad4pushVariables.LAST_CLICK_DATE,
      (col(Ad4pushVariables.CLICK_7) + col(MergeUtils.NEW_ + Ad4pushVariables.EFFECTIVE_7_DAYS)).cast(IntegerType) as Ad4pushVariables.CLICK_7,
      (col(Ad4pushVariables.CLICK_15) + col(MergeUtils.NEW_ + Ad4pushVariables.EFFECTIVE_15_DAYS)).cast(IntegerType) as Ad4pushVariables.CLICK_15,
      (col(Ad4pushVariables.CLICK_30) + col(MergeUtils.NEW_ + Ad4pushVariables.EFFECTIVE_30_DAYS)).cast(IntegerType) as Ad4pushVariables.CLICK_30,
      (col(Ad4pushVariables.CLICK_LIFETIME) + col(MergeUtils.NEW_ + Ad4pushVariables.CLICKED_TODAY)).cast(IntegerType) as Ad4pushVariables.CLICK_LIFETIME,
      (when(col(MergeUtils.NEW_ + Ad4pushVariables.CLICKED_TODAY) > 2, col(Ad4pushVariables.CLICKED_TWICE) + 1).otherwise(col(Ad4pushVariables.CLICKED_TWICE))).cast(IntegerType) as Ad4pushVariables.CLICKED_TWICE,

      (col(Ad4pushVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 0).toLowerCase()) + col(MergeUtils.NEW_ + Ad4pushVariables.CLICKED_TODAY)).cast(IntegerType) as Ad4pushVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 0).toLowerCase(),
      (col(Ad4pushVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 1).toLowerCase())).cast(IntegerType) as Ad4pushVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 1).toLowerCase(),
      (col(Ad4pushVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 2).toLowerCase())).cast(IntegerType) as Ad4pushVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 2).toLowerCase(),
      (col(Ad4pushVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 3).toLowerCase())).cast(IntegerType) as Ad4pushVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 3).toLowerCase(),
      (col(Ad4pushVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 4).toLowerCase())).cast(IntegerType) as Ad4pushVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 4).toLowerCase(),
      (col(Ad4pushVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 5).toLowerCase())).cast(IntegerType) as Ad4pushVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 5).toLowerCase(),
      (col(Ad4pushVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 6).toLowerCase())).cast(IntegerType) as Ad4pushVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 6).toLowerCase()
    )

    val result = resultDF.select(
      col(Ad4pushVariables.DEVICE_ID),
      col(Ad4pushVariables.CUSTOMER_ID),
      col(Ad4pushVariables.LAST_CLICK_DATE),
      col(Ad4pushVariables.CLICK_7),
      col(Ad4pushVariables.CLICK_15),
      col(Ad4pushVariables.CLICK_30),
      col(Ad4pushVariables.CLICK_LIFETIME),
      col(Ad4pushVariables.CLICK_MONDAY),
      col(Ad4pushVariables.CLICK_TUESDAY),
      col(Ad4pushVariables.CLICK_WEDNESDAY),
      col(Ad4pushVariables.CLICK_THURSDAY),
      col(Ad4pushVariables.CLICK_FRIDAY),
      col(Ad4pushVariables.CLICK_SATURDAY),
      col(Ad4pushVariables.CLICK_SUNDAY),
      col(Ad4pushVariables.CLICKED_TWICE),
      Udf.maxClickDayName(
        col(Ad4pushVariables.CLICK_MONDAY), col(Ad4pushVariables.CLICK_TUESDAY),
        col(Ad4pushVariables.CLICK_WEDNESDAY), col(Ad4pushVariables.CLICK_THURSDAY),
        col(Ad4pushVariables.CLICK_FRIDAY), col(Ad4pushVariables.CLICK_SATURDAY),
        col(Ad4pushVariables.CLICK_SUNDAY)
      ) as Ad4pushVariables.MOST_CLICK_DAY
    ).cache()
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

    val joined_7_15 = MergeUtils.joinOldAndNewDF(effective15, Ad4pushSchema.reducedDF, effective7,
      Ad4pushSchema.reducedDF, Ad4pushVariables.DEVICE_ID, Ad4pushVariables.CUSTOMER_ID)
      .na.fill(
        Map(
          Ad4pushVariables.REACTION -> 0,
          new_reaction -> 0
        )
      )
    val joined_7_15_summary = joined_7_15.select(
      coalesce(col(Ad4pushVariables.DEVICE_ID), col(MergeUtils.NEW_ + Ad4pushVariables.DEVICE_ID)) as Ad4pushVariables.DEVICE_ID,
      coalesce(col(Ad4pushVariables.CUSTOMER_ID), col(MergeUtils.NEW_ + Ad4pushVariables.CUSTOMER_ID)) as Ad4pushVariables.CUSTOMER_ID,
      col(Ad4pushVariables.REACTION) as Ad4pushVariables.EFFECTIVE_7_DAYS,
      col(new_reaction) as Ad4pushVariables.EFFECTIVE_15_DAYS)

    val joined_7_15_30 = MergeUtils.joinOldAndNewDF(effective30, Ad4pushSchema.reducedDF, joined_7_15_summary,
      Ad4pushSchema.joined_7_15, Ad4pushVariables.DEVICE_ID, Ad4pushVariables.CUSTOMER_ID)
      .na.fill(
        Map(
          Ad4pushVariables.EFFECTIVE_7_DAYS -> 0,
          Ad4pushVariables.EFFECTIVE_15_DAYS -> 0,
          new_reaction -> 0
        )
      )

    val joined_7_15_30_summary = joined_7_15_30.select(
      coalesce(col(MergeUtils.NEW_ + Ad4pushVariables.DEVICE_ID), col(Ad4pushVariables.DEVICE_ID)) as Ad4pushVariables.DEVICE_ID,
      coalesce(col(MergeUtils.NEW_ + Ad4pushVariables.CUSTOMER_ID), col(Ad4pushVariables.CUSTOMER_ID)) as Ad4pushVariables.CUSTOMER_ID,
      col(Ad4pushVariables.EFFECTIVE_7_DAYS) as Ad4pushVariables.EFFECTIVE_7_DAYS,
      col(Ad4pushVariables.EFFECTIVE_15_DAYS) as Ad4pushVariables.EFFECTIVE_15_DAYS,
      col(new_reaction) as Ad4pushVariables.EFFECTIVE_30_DAYS)

    val joinedAll = MergeUtils.joinOldAndNewDF(incremental, Ad4pushSchema.reducedDF, joined_7_15_30_summary, Ad4pushSchema.joined_7_15_30, Ad4pushVariables.DEVICE_ID, Ad4pushVariables.CUSTOMER_ID)
      .na.fill(
        Map(
          new_reaction -> 0,
          Ad4pushVariables.EFFECTIVE_7_DAYS -> 0,
          Ad4pushVariables.EFFECTIVE_15_DAYS -> 0,
          Ad4pushVariables.EFFECTIVE_30_DAYS -> 0
        )
      )
    val joinedAllSummary = joinedAll.select(
      coalesce(col(MergeUtils.NEW_ + Ad4pushVariables.DEVICE_ID), col(Ad4pushVariables.DEVICE_ID)) as Ad4pushVariables.DEVICE_ID,
      coalesce(col(MergeUtils.NEW_ + Ad4pushVariables.CUSTOMER_ID), col(Ad4pushVariables.CUSTOMER_ID)) as Ad4pushVariables.CUSTOMER_ID,
      col(new_reaction) - col(Ad4pushVariables.EFFECTIVE_7_DAYS) as Ad4pushVariables.EFFECTIVE_7_DAYS,
      col(new_reaction) - col(Ad4pushVariables.EFFECTIVE_15_DAYS) as Ad4pushVariables.EFFECTIVE_15_DAYS,
      col(new_reaction) - col(Ad4pushVariables.EFFECTIVE_30_DAYS) as Ad4pushVariables.EFFECTIVE_30_DAYS,
      col(new_reaction) as Ad4pushVariables.CLICKED_TODAY)

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
    return df.filter(col(Ad4pushVariables.REACTION).gt(0))
      .select(
        Ad4pushVariables.CUSTOMER_ID,
        Ad4pushVariables.DEVICE_ID,
        Ad4pushVariables.REACTION)
      .groupBy(Ad4pushVariables.DEVICE_ID, Ad4pushVariables.CUSTOMER_ID)
      .agg(sum(Ad4pushVariables.REACTION).cast(IntegerType) as Ad4pushVariables.REACTION)
      .select(Ad4pushVariables.CUSTOMER_ID, Ad4pushVariables.DEVICE_ID, Ad4pushVariables.REACTION)
  }

}
