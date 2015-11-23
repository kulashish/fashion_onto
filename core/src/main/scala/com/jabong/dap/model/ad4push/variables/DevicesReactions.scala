package com.jabong.dap.model.ad4push.variables

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.CustomerVariables
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

  val new_reaction = MergeUtils.NEW_ + CustomerVariables.REACTION

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
    df.select(Udf.removeAllZero(df(CustomerVariables.LOGIN_USER_ID)) as CustomerVariables.CUSTOMER_ID,
      Udf.removeAllZero(df(CustomerVariables.DEVICE_ID)) as CustomerVariables.DEVICE_ID,
      df(CustomerVariables.MESSAGE_ID) as CustomerVariables.MESSAGE_ID,
      df(CustomerVariables.CAMPAIGN_ID) as CustomerVariables.CAMPAIGN_ID,
      df(CustomerVariables.BOUNCE).cast(IntegerType) as CustomerVariables.BOUNCE,
      df(CustomerVariables.REACTION).cast(IntegerType) as CustomerVariables.REACTION)
      .filter(!(col(CustomerVariables.CUSTOMER_ID) === "" && col(CustomerVariables.DEVICE_ID) === ""))
      .filter(col(CustomerVariables.REACTION).gt(0) || col(CustomerVariables.BOUNCE).gt(0))
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

    val effective = effectiveDFFull(reducedIncr, reduced7, reduced15, reduced30).withColumnRenamed(CustomerVariables.CUSTOMER_ID, CustomerVariables.CUSTOMER_ID)

    val joinedDF = MergeUtils.joinOldAndNewDF(effective, Ad4pushSchema.effectiveDF, full,
      Ad4pushSchema.deviceReaction, CustomerVariables.DEVICE_ID, CustomerVariables.CUSTOMER_ID)
      .na.fill(
        Map(
          CustomerVariables.CLICK_7 -> 0,
          CustomerVariables.CLICK_15 -> 0,
          CustomerVariables.CLICK_30 -> 0,
          CustomerVariables.CLICK_LIFETIME -> 0,
          CustomerVariables.CLICKED_TWICE -> 0,
          CustomerVariables.CLICK_MONDAY -> 0,
          CustomerVariables.CLICK_TUESDAY -> 0,
          CustomerVariables.CLICK_WEDNESDAY -> 0,
          CustomerVariables.CLICK_THURSDAY -> 0,
          CustomerVariables.CLICK_FRIDAY -> 0,
          CustomerVariables.CLICK_SATURDAY -> 0,
          CustomerVariables.CLICK_SUNDAY -> 0,
          MergeUtils.NEW_ + CustomerVariables.EFFECTIVE_7_DAYS -> 0,
          MergeUtils.NEW_ + CustomerVariables.EFFECTIVE_15_DAYS -> 0,
          MergeUtils.NEW_ + CustomerVariables.EFFECTIVE_30_DAYS -> 0,
          MergeUtils.NEW_ + CustomerVariables.CLICKED_TODAY -> 0
        )
      )

    val resultDF = joinedDF.select(
      coalesce(col(MergeUtils.NEW_ + CustomerVariables.DEVICE_ID), col(CustomerVariables.DEVICE_ID)) as CustomerVariables.DEVICE_ID,
      coalesce(col(MergeUtils.NEW_ + CustomerVariables.CUSTOMER_ID), col(CustomerVariables.CUSTOMER_ID)) as CustomerVariables.CUSTOMER_ID,
      when(col(MergeUtils.NEW_ + CustomerVariables.CLICKED_TODAY) > 0, TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_FORMAT)).otherwise(col(CustomerVariables.LAST_CLICK_DATE)) as CustomerVariables.LAST_CLICK_DATE,
      (col(CustomerVariables.CLICK_7) + col(MergeUtils.NEW_ + CustomerVariables.EFFECTIVE_7_DAYS)).cast(IntegerType) as CustomerVariables.CLICK_7,
      (col(CustomerVariables.CLICK_15) + col(MergeUtils.NEW_ + CustomerVariables.EFFECTIVE_15_DAYS)).cast(IntegerType) as CustomerVariables.CLICK_15,
      (col(CustomerVariables.CLICK_30) + col(MergeUtils.NEW_ + CustomerVariables.EFFECTIVE_30_DAYS)).cast(IntegerType) as CustomerVariables.CLICK_30,
      (col(CustomerVariables.CLICK_LIFETIME) + col(MergeUtils.NEW_ + CustomerVariables.CLICKED_TODAY)).cast(IntegerType) as CustomerVariables.CLICK_LIFETIME,
      (when(col(MergeUtils.NEW_ + CustomerVariables.CLICKED_TODAY) > 2, col(CustomerVariables.CLICKED_TWICE) + 1).otherwise(col(CustomerVariables.CLICKED_TWICE))).cast(IntegerType) as CustomerVariables.CLICKED_TWICE,

      (col(CustomerVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 0).toLowerCase()) + col(MergeUtils.NEW_ + CustomerVariables.CLICKED_TODAY)).cast(IntegerType) as CustomerVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 0).toLowerCase(),
      (col(CustomerVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 1).toLowerCase())).cast(IntegerType) as CustomerVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 1).toLowerCase(),
      (col(CustomerVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 2).toLowerCase())).cast(IntegerType) as CustomerVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 2).toLowerCase(),
      (col(CustomerVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 3).toLowerCase())).cast(IntegerType) as CustomerVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 3).toLowerCase(),
      (col(CustomerVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 4).toLowerCase())).cast(IntegerType) as CustomerVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 4).toLowerCase(),
      (col(CustomerVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 5).toLowerCase())).cast(IntegerType) as CustomerVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 5).toLowerCase(),
      (col(CustomerVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 6).toLowerCase())).cast(IntegerType) as CustomerVariables.CLICK_ + TimeUtils.nextNDay(incrDay, 6).toLowerCase()
    )

    val result = resultDF.select(
      col(CustomerVariables.DEVICE_ID),
      col(CustomerVariables.CUSTOMER_ID),
      col(CustomerVariables.LAST_CLICK_DATE),
      col(CustomerVariables.CLICK_7),
      col(CustomerVariables.CLICK_15),
      col(CustomerVariables.CLICK_30),
      col(CustomerVariables.CLICK_LIFETIME),
      col(CustomerVariables.CLICK_MONDAY),
      col(CustomerVariables.CLICK_TUESDAY),
      col(CustomerVariables.CLICK_WEDNESDAY),
      col(CustomerVariables.CLICK_THURSDAY),
      col(CustomerVariables.CLICK_FRIDAY),
      col(CustomerVariables.CLICK_SATURDAY),
      col(CustomerVariables.CLICK_SUNDAY),
      col(CustomerVariables.CLICKED_TWICE),
      Udf.maxClickDayName(
        col(CustomerVariables.CLICK_MONDAY), col(CustomerVariables.CLICK_TUESDAY),
        col(CustomerVariables.CLICK_WEDNESDAY), col(CustomerVariables.CLICK_THURSDAY),
        col(CustomerVariables.CLICK_FRIDAY), col(CustomerVariables.CLICK_SATURDAY),
        col(CustomerVariables.CLICK_SUNDAY)
      ) as CustomerVariables.MOST_CLICK_DAY
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
      Ad4pushSchema.reducedDF, CustomerVariables.DEVICE_ID, CustomerVariables.CUSTOMER_ID)
      .na.fill(
        Map(
          CustomerVariables.REACTION -> 0,
          new_reaction -> 0
        )
      )
    val joined_7_15_summary = joined_7_15.select(
      coalesce(col(CustomerVariables.DEVICE_ID), col(MergeUtils.NEW_ + CustomerVariables.DEVICE_ID)) as CustomerVariables.DEVICE_ID,
      coalesce(col(CustomerVariables.CUSTOMER_ID), col(MergeUtils.NEW_ + CustomerVariables.CUSTOMER_ID)) as CustomerVariables.CUSTOMER_ID,
      col(CustomerVariables.REACTION) as CustomerVariables.EFFECTIVE_7_DAYS,
      col(new_reaction) as CustomerVariables.EFFECTIVE_15_DAYS)

    val joined_7_15_30 = MergeUtils.joinOldAndNewDF(effective30, Ad4pushSchema.reducedDF, joined_7_15_summary,
      Ad4pushSchema.joined_7_15, CustomerVariables.DEVICE_ID, CustomerVariables.CUSTOMER_ID)
      .na.fill(
        Map(
          CustomerVariables.EFFECTIVE_7_DAYS -> 0,
          CustomerVariables.EFFECTIVE_15_DAYS -> 0,
          new_reaction -> 0
        )
      )

    val joined_7_15_30_summary = joined_7_15_30.select(
      coalesce(col(MergeUtils.NEW_ + CustomerVariables.DEVICE_ID), col(CustomerVariables.DEVICE_ID)) as CustomerVariables.DEVICE_ID,
      coalesce(col(MergeUtils.NEW_ + CustomerVariables.CUSTOMER_ID), col(CustomerVariables.CUSTOMER_ID)) as CustomerVariables.CUSTOMER_ID,
      col(CustomerVariables.EFFECTIVE_7_DAYS) as CustomerVariables.EFFECTIVE_7_DAYS,
      col(CustomerVariables.EFFECTIVE_15_DAYS) as CustomerVariables.EFFECTIVE_15_DAYS,
      col(new_reaction) as CustomerVariables.EFFECTIVE_30_DAYS)

    val joinedAll = MergeUtils.joinOldAndNewDF(incremental, Ad4pushSchema.reducedDF, joined_7_15_30_summary, Ad4pushSchema.joined_7_15_30, CustomerVariables.DEVICE_ID, CustomerVariables.CUSTOMER_ID)
      .na.fill(
        Map(
          new_reaction -> 0,
          CustomerVariables.EFFECTIVE_7_DAYS -> 0,
          CustomerVariables.EFFECTIVE_15_DAYS -> 0,
          CustomerVariables.EFFECTIVE_30_DAYS -> 0
        )
      )
    val joinedAllSummary = joinedAll.select(
      coalesce(col(MergeUtils.NEW_ + CustomerVariables.DEVICE_ID), col(CustomerVariables.DEVICE_ID)) as CustomerVariables.DEVICE_ID,
      coalesce(col(MergeUtils.NEW_ + CustomerVariables.CUSTOMER_ID), col(CustomerVariables.CUSTOMER_ID)) as CustomerVariables.CUSTOMER_ID,
      col(new_reaction) - col(CustomerVariables.EFFECTIVE_7_DAYS) as CustomerVariables.EFFECTIVE_7_DAYS,
      col(new_reaction) - col(CustomerVariables.EFFECTIVE_15_DAYS) as CustomerVariables.EFFECTIVE_15_DAYS,
      col(new_reaction) - col(CustomerVariables.EFFECTIVE_30_DAYS) as CustomerVariables.EFFECTIVE_30_DAYS,
      col(new_reaction) as CustomerVariables.CLICKED_TODAY)

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
    return df.filter(col(CustomerVariables.REACTION).gt(0))
      .select(
        CustomerVariables.CUSTOMER_ID,
        CustomerVariables.DEVICE_ID,
        CustomerVariables.REACTION)
      .groupBy(CustomerVariables.DEVICE_ID, CustomerVariables.CUSTOMER_ID)
      .agg(sum(CustomerVariables.REACTION).cast(IntegerType) as CustomerVariables.REACTION)
      .select(CustomerVariables.CUSTOMER_ID, CustomerVariables.DEVICE_ID, CustomerVariables.REACTION)
  }

}
