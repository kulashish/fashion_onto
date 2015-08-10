package com.jabong.dap.model.ad4push.variables

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.constants.campaign.CampaignMergedFields
import com.jabong.dap.common.constants.variables.DevicesReactionsVariables
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.MergeUtils
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.ad4push.schema.DevicesReactionsSchema
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
 * Created by Kapil.Rajak on 13/7/15.
 */
object DevicesReactions extends Logging {

  val new_reaction = MergeUtils.NEW_ + DevicesReactionsVariables.REACTION

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

    val savePathI = DataWriter.getWritePath(DataSets.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_IOS, DataSets.FULL_MERGE_MODE, incrDate)

    var unionFinalDF: DataFrame = null

    if (DataWriter.canWrite(savePathI, saveMode)) {
      val fName = "exportMessagesReactions_" + CampaignMergedFields.IOS_CODE + "_" + incrDateInFileFormat + ".csv"
      val incIStringSchema = DataReader.getDataFrame4mCsv(DataSets.INPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_IOS, DataSets.DAILY_MODE, incrDate, fName, "true", ",")
      val incI = dfCorrectSchema(incIStringSchema)

      //getting DF
      logger.info("Reading inputs (CSVs and Parquets) for IOS")
      val fullI = DataReader.getDataFrameOrNull(DataSets.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_IOS, DataSets.FULL_MERGE_MODE, yesterday)
      val b7I = DataReader.getDataFrameOrNull(DataSets.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_IOS, DataSets.DAILY_MODE, before7daysString)
      val b15I = DataReader.getDataFrameOrNull(DataSets.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_IOS, DataSets.DAILY_MODE, before15daysString)
      val b30I = DataReader.getDataFrameOrNull(DataSets.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_IOS, DataSets.DAILY_MODE, before30daysString)

      val (resultI, incrI) = fullSummary(incI, incrDate, fullI, b7I, b15I, b30I)

      val incrSavePathI = DataWriter.getWritePath(DataSets.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_IOS, DataSets.DAILY_MODE, incrDate)
      if (DataWriter.canWrite(incrSavePathI, saveMode)) {
        DataWriter.writeParquet(incrI, incrSavePathI, saveMode)
      }

      DataWriter.writeParquet(resultI, savePathI, saveMode)

      unionFinalDF = resultI

      //      val filename = DataSets.AD4PUSH + "_" + DataSets.CUSTOMER_RESPONSE + "_" + DataSets.IOS + "_" + incrDateInFileFormat
      //
      //      DataWriter.writeCsv(resultI, DataSets.AD4PUSH, DataSets.REACTIONS_IOS_CSV, DataSets.FULL_MERGE_MODE, incrDate, filename, "true", ",")
    }

    val savePathA = DataWriter.getWritePath(DataSets.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_ANDROID, DataSets.FULL_MERGE_MODE, incrDate)
    if (DataWriter.canWrite(savePathA, saveMode)) {
      val fName = "exportMessagesReactions_" + CampaignMergedFields.ANDROID_CODE + "_" + incrDateInFileFormat + ".csv"
      val incAStringSchema = DataReader.getDataFrame4mCsv(DataSets.INPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_ANDROID, DataSets.DAILY_MODE, incrDate, fName, "true", ",")
      val incA = dfCorrectSchema(incAStringSchema)

      //getting DF
      logger.info("Reading inputs (CSVs and Parquets) for Android")
      val fullA = DataReader.getDataFrameOrNull(DataSets.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_ANDROID, DataSets.FULL_MERGE_MODE, yesterday)
      val b7A = DataReader.getDataFrameOrNull(DataSets.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_ANDROID, DataSets.DAILY_MODE, before7daysString)
      val b15A = DataReader.getDataFrameOrNull(DataSets.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_ANDROID, DataSets.DAILY_MODE, before15daysString)
      val b30A = DataReader.getDataFrameOrNull(DataSets.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_ANDROID, DataSets.DAILY_MODE, before30daysString)

      val (resultA, incrA) = fullSummary(incA, incrDate, fullA, b7A, b15A, b30A)

      val incrSavePathA = DataWriter.getWritePath(DataSets.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.REACTIONS_ANDROID, DataSets.DAILY_MODE, incrDate)
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
    df.select(Udf.removeAllZero(df(DevicesReactionsVariables.LOGIN_USER_ID)) as DevicesReactionsVariables.CUSTOMER_ID,
      Udf.removeAllZero(df(DevicesReactionsVariables.DEVICE_ID)) as DevicesReactionsVariables.DEVICE_ID,
      df(DevicesReactionsVariables.MESSAGE_ID) as DevicesReactionsVariables.MESSAGE_ID,
      df(DevicesReactionsVariables.CAMPAIGN_ID) as DevicesReactionsVariables.CAMPAIGN_ID,
      df(DevicesReactionsVariables.BOUNCE).cast(IntegerType) as DevicesReactionsVariables.BOUNCE,
      df(DevicesReactionsVariables.REACTION).cast(IntegerType) as DevicesReactionsVariables.REACTION)
      .filter(!(col(DevicesReactionsVariables.CUSTOMER_ID) === "" && col(DevicesReactionsVariables.DEVICE_ID) === ""))
      .filter(col(DevicesReactionsVariables.REACTION).gt(0) || col(DevicesReactionsVariables.BOUNCE).gt(0))
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

    val effective = effectiveDFFull(reducedIncr, reduced7, reduced15, reduced30).withColumnRenamed(DevicesReactionsVariables.CUSTOMER_ID, DevicesReactionsVariables.CUSTOMER_ID)

    val joinedDF = MergeUtils.joinOldAndNewDF(effective, DevicesReactionsSchema.effectiveDF, full,
      DevicesReactionsSchema.deviceReaction, DevicesReactionsVariables.DEVICE_ID, DevicesReactionsVariables.CUSTOMER_ID)
      .na.fill(
        Map(
          DevicesReactionsVariables.CLICK_7 -> 0,
          DevicesReactionsVariables.CLICK_15 -> 0,
          DevicesReactionsVariables.CLICK_30 -> 0,
          DevicesReactionsVariables.CLICK_LIFETIME -> 0,
          DevicesReactionsVariables.CLICKED_TWICE -> 0,
          DevicesReactionsVariables.CLICK_MONDAY -> 0,
          DevicesReactionsVariables.CLICK_TUESDAY -> 0,
          DevicesReactionsVariables.CLICK_WEDNESDAY -> 0,
          DevicesReactionsVariables.CLICK_THURSDAY -> 0,
          DevicesReactionsVariables.CLICK_FRIDAY -> 0,
          DevicesReactionsVariables.CLICK_SATURDAY -> 0,
          DevicesReactionsVariables.CLICK_SUNDAY -> 0,
          MergeUtils.NEW_ + DevicesReactionsVariables.EFFECTIVE_7_DAYS -> 0,
          MergeUtils.NEW_ + DevicesReactionsVariables.EFFECTIVE_15_DAYS -> 0,
          MergeUtils.NEW_ + DevicesReactionsVariables.EFFECTIVE_30_DAYS -> 0,
          MergeUtils.NEW_ + DevicesReactionsVariables.CLICKED_TODAY -> 0
        )
      )

    val resultDF = joinedDF.select(
      coalesce(col(MergeUtils.NEW_ + DevicesReactionsVariables.DEVICE_ID), col(DevicesReactionsVariables.DEVICE_ID)) as DevicesReactionsVariables.DEVICE_ID,
      coalesce(col(MergeUtils.NEW_ + DevicesReactionsVariables.CUSTOMER_ID), col(DevicesReactionsVariables.CUSTOMER_ID)) as DevicesReactionsVariables.CUSTOMER_ID,
      when(col(MergeUtils.NEW_ + DevicesReactionsVariables.CLICKED_TODAY) > 0, TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_FORMAT)).otherwise(col(DevicesReactionsVariables.LAST_CLICK_DATE)) as DevicesReactionsVariables.LAST_CLICK_DATE,
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

    val result = resultDF.select(
      col(DevicesReactionsVariables.DEVICE_ID),
      col(DevicesReactionsVariables.CUSTOMER_ID),
      col(DevicesReactionsVariables.LAST_CLICK_DATE),
      col(DevicesReactionsVariables.CLICK_7),
      col(DevicesReactionsVariables.CLICK_15),
      col(DevicesReactionsVariables.CLICK_30),
      col(DevicesReactionsVariables.CLICK_LIFETIME),
      col(DevicesReactionsVariables.CLICK_MONDAY),
      col(DevicesReactionsVariables.CLICK_TUESDAY),
      col(DevicesReactionsVariables.CLICK_WEDNESDAY),
      col(DevicesReactionsVariables.CLICK_THURSDAY),
      col(DevicesReactionsVariables.CLICK_FRIDAY),
      col(DevicesReactionsVariables.CLICK_SATURDAY),
      col(DevicesReactionsVariables.CLICK_SUNDAY),
      col(DevicesReactionsVariables.CLICKED_TWICE),
      Udf.maxClickDayName(
        col(DevicesReactionsVariables.CLICK_MONDAY), col(DevicesReactionsVariables.CLICK_TUESDAY),
        col(DevicesReactionsVariables.CLICK_WEDNESDAY), col(DevicesReactionsVariables.CLICK_THURSDAY),
        col(DevicesReactionsVariables.CLICK_FRIDAY), col(DevicesReactionsVariables.CLICK_SATURDAY),
        col(DevicesReactionsVariables.CLICK_SUNDAY)
      ) as DevicesReactionsVariables.MOST_CLICK_DAY
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

    val joined_7_15 = MergeUtils.joinOldAndNewDF(effective15, DevicesReactionsSchema.reducedDF, effective7,
      DevicesReactionsSchema.reducedDF, DevicesReactionsVariables.DEVICE_ID, DevicesReactionsVariables.CUSTOMER_ID)
      .na.fill(
        Map(
          DevicesReactionsVariables.REACTION -> 0,
          new_reaction -> 0
        )
      )
    val joined_7_15_summary = joined_7_15.select(
      coalesce(col(DevicesReactionsVariables.DEVICE_ID), col(MergeUtils.NEW_ + DevicesReactionsVariables.DEVICE_ID)) as DevicesReactionsVariables.DEVICE_ID,
      coalesce(col(DevicesReactionsVariables.CUSTOMER_ID), col(MergeUtils.NEW_ + DevicesReactionsVariables.CUSTOMER_ID)) as DevicesReactionsVariables.CUSTOMER_ID,
      col(DevicesReactionsVariables.REACTION) as DevicesReactionsVariables.EFFECTIVE_7_DAYS,
      col(new_reaction) as DevicesReactionsVariables.EFFECTIVE_15_DAYS)

    val joined_7_15_30 = MergeUtils.joinOldAndNewDF(effective30, DevicesReactionsSchema.reducedDF, joined_7_15_summary,
      DevicesReactionsSchema.joined_7_15, DevicesReactionsVariables.DEVICE_ID, DevicesReactionsVariables.CUSTOMER_ID)
      .na.fill(
        Map(
          DevicesReactionsVariables.EFFECTIVE_7_DAYS -> 0,
          DevicesReactionsVariables.EFFECTIVE_15_DAYS -> 0,
          new_reaction -> 0
        )
      )

    val joined_7_15_30_summary = joined_7_15_30.select(
      coalesce(col(MergeUtils.NEW_ + DevicesReactionsVariables.DEVICE_ID), col(DevicesReactionsVariables.DEVICE_ID)) as DevicesReactionsVariables.DEVICE_ID,
      coalesce(col(MergeUtils.NEW_ + DevicesReactionsVariables.CUSTOMER_ID), col(DevicesReactionsVariables.CUSTOMER_ID)) as DevicesReactionsVariables.CUSTOMER_ID,
      col(DevicesReactionsVariables.EFFECTIVE_7_DAYS) as DevicesReactionsVariables.EFFECTIVE_7_DAYS,
      col(DevicesReactionsVariables.EFFECTIVE_15_DAYS) as DevicesReactionsVariables.EFFECTIVE_15_DAYS,
      col(new_reaction) as DevicesReactionsVariables.EFFECTIVE_30_DAYS)

    val joinedAll = MergeUtils.joinOldAndNewDF(incremental, DevicesReactionsSchema.reducedDF, joined_7_15_30_summary, DevicesReactionsSchema.joined_7_15_30, DevicesReactionsVariables.DEVICE_ID, DevicesReactionsVariables.CUSTOMER_ID)
      .na.fill(
        Map(
          new_reaction -> 0,
          DevicesReactionsVariables.EFFECTIVE_7_DAYS -> 0,
          DevicesReactionsVariables.EFFECTIVE_15_DAYS -> 0,
          DevicesReactionsVariables.EFFECTIVE_30_DAYS -> 0
        )
      )
    val joinedAllSummary = joinedAll.select(
      coalesce(col(MergeUtils.NEW_ + DevicesReactionsVariables.DEVICE_ID), col(DevicesReactionsVariables.DEVICE_ID)) as DevicesReactionsVariables.DEVICE_ID,
      coalesce(col(MergeUtils.NEW_ + DevicesReactionsVariables.CUSTOMER_ID), col(DevicesReactionsVariables.CUSTOMER_ID)) as DevicesReactionsVariables.CUSTOMER_ID,
      col(new_reaction) - col(DevicesReactionsVariables.EFFECTIVE_7_DAYS) as DevicesReactionsVariables.EFFECTIVE_7_DAYS,
      col(new_reaction) - col(DevicesReactionsVariables.EFFECTIVE_15_DAYS) as DevicesReactionsVariables.EFFECTIVE_15_DAYS,
      col(new_reaction) - col(DevicesReactionsVariables.EFFECTIVE_30_DAYS) as DevicesReactionsVariables.EFFECTIVE_30_DAYS,
      col(new_reaction) as DevicesReactionsVariables.CLICKED_TODAY)

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
    return df.filter(col(DevicesReactionsVariables.REACTION).gt(0))
      .select(
        DevicesReactionsVariables.CUSTOMER_ID,
        DevicesReactionsVariables.DEVICE_ID,
        DevicesReactionsVariables.REACTION)
      .groupBy(DevicesReactionsVariables.DEVICE_ID, DevicesReactionsVariables.CUSTOMER_ID)
      .agg(sum(DevicesReactionsVariables.REACTION).cast(IntegerType) as DevicesReactionsVariables.REACTION)
      .select(DevicesReactionsVariables.CUSTOMER_ID, DevicesReactionsVariables.DEVICE_ID, DevicesReactionsVariables.REACTION)
  }

}
