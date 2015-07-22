package com.jabong.dap.model.ad4push.variables

import java.text.SimpleDateFormat
import java.util.Calendar
import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.variables.DevicesReactionsVariables._
import com.jabong.dap.common.time.{Constants, TimeUtils}
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
object DevicesReactions extends Logging{
  /**
   * @param path
   * @return DataFrame from the path
   */
  def getDF(path: String): DataFrame = {
    var df: DataFrame = null
    try {
      df = Spark.getSqlContext().read.parquet(path)
    }catch {
      case ae: AssertionError => logger.info(ae.getMessage)
    }
    return df
  }

  /**
   * @param path
   * @return true if success else false
   */
  def writeDF(df: DataFrame, path: String) {
    df.write.parquet(path)
  }

  /**
   *All read CSV, read perquet, write perquet
   * @param yyyyMMdd date for which summary is needed in YYYYMMDD format
   * @return (iPhoneResult, AndroidResult) for tgiven date
   */
  def io(yyyyMMdd: String): (DataFrame, DataFrame) = {

    //getting file names
    val today = Calendar.getInstance().getTime()
    val formatter = new SimpleDateFormat(Constants.YYYYMMDD)
    val dateString = if(yyyyMMdd != null) yyyyMMdd else formatter.format(today)
    val incIPhoneCSV = DataSets.IPHONE_CSV_PREFIX + dateString + DataSets.CSV_EXTENSION
    val incAndroidCSV = DataSets.ANDROID_CSV_PREFIX + dateString  + DataSets.CSV_EXTENSION

    val date = if(yyyyMMdd != null) formatter.parse(yyyyMMdd) else today

    val cal = Calendar.getInstance()

    cal.setTime(date)
    cal.add(Calendar.DATE, -1)
    val yesterday = formatter.format(cal.getTime)
    val fullIPhoneFile = DataSets.IPHONE_DF_PREFIX + yesterday
    val fullAndroidFile = DataSets.ANDROID_DF_PREFIX + yesterday

    val resultIFile = DataSets.IPHONE_DF_PREFIX+dateString
    val resultAFile = DataSets.ANDROID_DF_PREFIX+dateString

    cal.setTime(date)
    cal.add(Calendar.DATE, -8)
    val before7daysString = formatter.format(cal.getTime)
    val before7daysIPhoneCSV = DataSets.IPHONE_CSV_PREFIX + before7daysString  + DataSets.CSV_EXTENSION
    val before7daysAndroidCSV = DataSets.ANDROID_CSV_PREFIX + before7daysString + DataSets.CSV_EXTENSION

    cal.setTime(date)
    cal.add(Calendar.DATE, -16)
    val before15daysString = formatter.format(cal.getTime)
    val before15daysIPhoneCSV = DataSets.IPHONE_CSV_PREFIX + before15daysString + DataSets.CSV_EXTENSION
    val before15daysAndroidCSV = DataSets.ANDROID_CSV_PREFIX + before15daysString + DataSets.CSV_EXTENSION

    cal.setTime(date)
    cal.add(Calendar.DATE, -31)
    val before30daysString = formatter.format(cal.getTime)
    val before30daysIPhoneCSV = DataSets.IPHONE_CSV_PREFIX + before30daysString + DataSets.CSV_EXTENSION
    val before30daysAndroidCSV = DataSets.ANDROID_CSV_PREFIX + before30daysString + DataSets.CSV_EXTENSION

    //getting path
    val incIPhonePath = DataSets.DEVICE_REACTION_CSV_DIRECTORY+"/"+incIPhoneCSV
    val fullIPhonePath = DataSets.DEVICE_REACTION_DF_DIRECTORY+"/"+fullIPhoneFile+DataSets.PARQUET_EXTENSION
    val before7daysIPhone = DataSets.DEVICE_REACTION_CSV_DIRECTORY+"/"+before7daysIPhoneCSV
    val before15daysIPhone = DataSets.DEVICE_REACTION_CSV_DIRECTORY+"/"+before15daysIPhoneCSV
    val before30daysIPhone = DataSets.DEVICE_REACTION_CSV_DIRECTORY+"/"+before30daysIPhoneCSV

    val incAndroidPath = DataSets.DEVICE_REACTION_CSV_DIRECTORY+"/"+ incAndroidCSV
    val fullAndroidPath = DataSets.DEVICE_REACTION_DF_DIRECTORY+"/"+ fullAndroidFile+DataSets.PARQUET_EXTENSION
    val before7daysAndroid = DataSets.DEVICE_REACTION_CSV_DIRECTORY+"/"+before7daysAndroidCSV
    val before15daysAndroid = DataSets.DEVICE_REACTION_CSV_DIRECTORY+"/"+before15daysAndroidCSV
    val before30daysAndroid = DataSets.DEVICE_REACTION_CSV_DIRECTORY+"/"+before30daysAndroidCSV

    val resultAPath = DataSets.DEVICE_REACTION_DF_DIRECTORY +"/"+ resultAFile + DataSets.PARQUET_EXTENSION
    val resultIPath = DataSets.DEVICE_REACTION_DF_DIRECTORY +"/"+ resultIFile + DataSets.PARQUET_EXTENSION

    //getting DF
    logger.info("Reading inputs (CSVs and Parquets)")
    val incI = dataFrameFromCsvPath(incIPhonePath)
    val fullI = getDF(fullIPhonePath)
    val b7I = dataFrameFromCsvPath(before7daysIPhone)
    val b15I = dataFrameFromCsvPath(before15daysIPhone)
    val b30I = dataFrameFromCsvPath(before30daysIPhone)

    val incA = dataFrameFromCsvPath(incAndroidPath)
    val fullA = getDF(fullAndroidPath)
    val b7A = dataFrameFromCsvPath(before7daysAndroid)
    val b15A = dataFrameFromCsvPath(before15daysAndroid)
    val b30A = dataFrameFromCsvPath(before30daysAndroid)

    val resultI = fullSummary(incI,dateString,fullI,b7I,b15I,b30I)
    val resultA = fullSummary(incA,dateString,fullA,b7A,b15A,b30A)

    writeDF(resultI,resultAPath)
    writeDF(resultA, resultIPath)
    return (resultI, resultA)
  }

  /**
   *
   * @param incrementalDF
   * @param toDay Today's date in YYYYMMDD format
   * @param full yesterday's perquet
   * @param before7days 8day's before DF
   * @param before15days 16 day's before DF
   * @param before30days 31 day's before DF
   * @return fullDF
   */
  def fullSummary(incrementalDF: DataFrame, toDay: String, full: DataFrame, before7days: DataFrame, before15days: DataFrame, before30days: DataFrame) : DataFrame = {

    logger.info("Processing DeviceReaction for :" +toDay)

    if(incrementalDF == null) {
      logger.info("Incremental DataFrame is null, returning full")
      return full
    }
    val toDayName = TimeUtils.dayName(toDay,Constants.YYYYMMDD).toLowerCase

    val reducedIncremental = reduce(incrementalDF)
    val reduced7 = reduce(before7days)
    val reduced15 = reduce(before15days)
    val reduced30 = reduce(before30days)


    val effective = effectiveDFFull(reducedIncremental, reduced7, reduced15, reduced30)

    val joinedDF = MergeUtils.joinOldAndNewDF(effective, DevicesReactionsSchema.effectiveDF, full, DevicesReactionsSchema.deviceReaction, DEVICE_ID)

    val resultDF = joinedDF.select(
                    coalesce(col(MergeUtils.NEW_ + DEVICE_ID),col(DEVICE_ID)) as DEVICE_ID,
                    coalesce(col(MergeUtils.NEW_ + LOGIN_USER_ID),col(CUSTOMER_ID)) as CUSTOMER_ID,
                    when(col(MergeUtils.NEW_ + CLICKED_TODAY)>0,TimeUtils.changeDateFormat(toDay, Constants.YYYYMMDD, Constants.DATE_FORMAT)).otherwise(col(LAST_CLICK_DATE)) as LAST_CLICK_DATE,
                    (col(CLICK_7)+col(MergeUtils.NEW_ + EFFECTIVE_7_DAYS)).cast(IntegerType) as CLICK_7,
                    (col(CLICK_15)+col(MergeUtils.NEW_ + EFFECTIVE_15_DAYS)).cast(IntegerType) as CLICK_15,
                    (col(CLICK_30)+col(MergeUtils.NEW_ + EFFECTIVE_30_DAYS)).cast(IntegerType) as CLICK_30,
                    (col(CLICK_LIFETIME)+col(MergeUtils.NEW_ + CLICKED_TODAY)).cast(IntegerType) as CLICK_LIFETIME,
                    (when(col(MergeUtils.NEW_ + CLICKED_TODAY) > 2, col(CLICKED_TWICE) +1).otherwise(col(CLICKED_TWICE))).cast(IntegerType) as CLICKED_TWICE,

                    (col(CLICK_ + TimeUtils.nextNDay(toDayName, 0).toLowerCase()) + col(MergeUtils.NEW_ + CLICKED_TODAY)).cast(IntegerType) as CLICK_ + TimeUtils.nextNDay(toDayName, 0).toLowerCase(),
                    (col(CLICK_ + TimeUtils.nextNDay(toDayName, 1).toLowerCase())).cast(IntegerType) as CLICK_ + TimeUtils.nextNDay(toDayName, 1).toLowerCase(),
                    (col(CLICK_ + TimeUtils.nextNDay(toDayName, 2).toLowerCase())).cast(IntegerType) as CLICK_ + TimeUtils.nextNDay(toDayName, 2).toLowerCase(),
                    (col(CLICK_ + TimeUtils.nextNDay(toDayName, 3).toLowerCase())).cast(IntegerType) as CLICK_ + TimeUtils.nextNDay(toDayName, 3).toLowerCase(),
                    (col(CLICK_ + TimeUtils.nextNDay(toDayName, 4).toLowerCase())).cast(IntegerType) as CLICK_ + TimeUtils.nextNDay(toDayName, 4).toLowerCase(),
                    (col(CLICK_ + TimeUtils.nextNDay(toDayName, 5).toLowerCase())).cast(IntegerType) as CLICK_ + TimeUtils.nextNDay(toDayName, 5).toLowerCase(),
                    (col(CLICK_ + TimeUtils.nextNDay(toDayName, 6).toLowerCase())).cast(IntegerType) as CLICK_ + TimeUtils.nextNDay(toDayName, 6).toLowerCase()
                    )

    val result = resultDF.select(col(DEVICE_ID), col(CUSTOMER_ID), col(LAST_CLICK_DATE), col(CLICK_7), col(CLICK_15), col(CLICK_30), col(CLICK_LIFETIME),
                    col(CLICK_MONDAY), col(CLICK_TUESDAY), col(CLICK_WEDNESDAY), col(CLICK_THURSDAY),
                    col(CLICK_FRIDAY), col(CLICK_SATURDAY), col(CLICK_SUNDAY), col(CLICKED_TWICE),
                    Udf.maxClickDayName(col(CLICK_MONDAY), col(CLICK_TUESDAY), col(CLICK_WEDNESDAY), col(CLICK_THURSDAY), col(CLICK_FRIDAY), col(CLICK_SATURDAY), col(CLICK_SUNDAY)) as MOST_CLICK_DAY
                    )
    logger.info("DeviceReaction for :" +toDay + "processed")
    return result
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

    val joined_7_15 = MergeUtils.joinOldAndNewDF(effective15, DevicesReactionsSchema.reducedDF, effective7, DevicesReactionsSchema.reducedDF,DEVICE_ID)
    val joined_7_15_summary = joined_7_15.select(
              coalesce(col(DEVICE_ID),col(MergeUtils.NEW_ + DEVICE_ID)) as DEVICE_ID,
              coalesce(col(LOGIN_USER_ID),col(LOGIN_USER_ID)) as LOGIN_USER_ID,
              col(REACTION) as EFFECTIVE_7_DAYS,
              col(MergeUtils.NEW_ + REACTION) as EFFECTIVE_15_DAYS)
            .na.fill(0)

    val joined_7_15_30 = MergeUtils.joinOldAndNewDF(effective30, DevicesReactionsSchema.reducedDF, joined_7_15_summary, DevicesReactionsSchema.joined_7_15,DEVICE_ID)
    val joined_7_15_30_summary = joined_7_15_30.select(
              coalesce(col(MergeUtils.NEW_ + DEVICE_ID),col(DEVICE_ID)) as DEVICE_ID,
              coalesce(col(MergeUtils.NEW_ + LOGIN_USER_ID),col(LOGIN_USER_ID)) as LOGIN_USER_ID,
              col(EFFECTIVE_7_DAYS) as EFFECTIVE_7_DAYS,
              col(EFFECTIVE_15_DAYS) as EFFECTIVE_15_DAYS,
              col(MergeUtils.NEW_ + REACTION) as EFFECTIVE_30_DAYS)
      .na.fill(0)

    val joinedAll = MergeUtils.joinOldAndNewDF(incremental, DevicesReactionsSchema.reducedDF, joined_7_15_30_summary, DevicesReactionsSchema.joined_7_15_30, DEVICE_ID)
    val joinedAllSummary = joinedAll.select(
      coalesce(col(MergeUtils.NEW_ + DEVICE_ID),col(DEVICE_ID)) as DEVICE_ID,
      coalesce(col(MergeUtils.NEW_ + LOGIN_USER_ID),col(LOGIN_USER_ID)) as LOGIN_USER_ID,
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
    if(df == null)  {
      logger.info("DataFrame df is null, returning null")
      return null
    }
    return df.select(LOGIN_USER_ID,DEVICE_ID,REACTION).groupBy(DEVICE_ID).agg(max(LOGIN_USER_ID) as LOGIN_USER_ID, sum(REACTION).cast(IntegerType) as REACTION)
            .select(LOGIN_USER_ID, DEVICE_ID, REACTION)
  }

  /**
   * @param path
   * @return df for the CSV with given path
   */
  def dataFrameFromCsvPath(path: String): DataFrame =  {
    var df: DataFrame = null
    try {
      df = Spark.getSqlContext().read.format("com.databricks.spark.csv").option("header", "true").load(path)
    } catch {
      case iie: InvalidInputException => logger.info(iie.getMessage)
        return null
    }
    df.select(col(LOGIN_USER_ID),col(DEVICE_ID),col(MESSAGE_ID),col(CAMPAIGN_ID),col(BOUNCE).cast(IntegerType) as BOUNCE,col(REACTION).cast(IntegerType) as REACTION)
  }

}
