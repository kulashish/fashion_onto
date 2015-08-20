package com.jabong.dap.export.dcf

import java.sql.Timestamp

import com.jabong.dap.common.{ Spark, OptionUtils }
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by rahul on 20/8/15.
 */
object DcfFeedGenerator extends Logging {

  def start(params: ParamInfo) {
    val hiveContext = Spark.getHiveContext()
    val executeDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val saveMode = params.saveMode
    val clickstreamTable = "merge.merge_pagevisit"
    val monthYear = TimeUtils.getMonthAndYear(executeDate, TimeConstants.DATE_FORMAT_FOLDER)
    val month = monthYear.month + 1
    val date = monthYear.day
    val year = monthYear.year
    val dateFolder = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)

    val cmr = DataReader.getDataFrame(ConfigConstants.OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, executeDate)
    //    val monthWithZero = withLeadingZeros(month)
    //    val dateWithZero = withLeadingZeros(date)
    //    println("/data/output/extras/device_mapping/full/"+year+"/"+monthWithZero+"/"+dateWithZero)
    //    val deviceMapping = hiveContext.read.parquet("/data/output/extras/device_mapping/full/"+year+"/"+monthWithZero+"/"+dateWithZero+"/*/")
    println("deviceMapping" + cmr.count())

    println("SELECT userid, productsku,pagets,sessionid FROM " + clickstreamTable +
      " where pagetype in ('CPD','QPD','DPD') and date1 = " + date + " and month1 = " + month + " and year1=" + year)

    val pageVisitData = hiveContext.sql("SELECT userid, productsku,pagets,sessionid FROM " + clickstreamTable +
      " where pagetype in ('CPD','QPD','DPD') and userid is not null and date1 = " + date + " and month1 = " + month + " and year1=" + year)

    println("pageVisitCount" + pageVisitData.count)

    val joinedData = convertFeedFormat(pageVisitData, cmr)
    val changedDateFormat = TimeUtils.changeDateFormat(executeDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_FORMAT)
    val writePath = DataWriter.getWritePath(ConfigConstants.OUTPUT_PATH, DataSets.DCF_FEED, DataSets.CLICKSTREAM_MERGED_FEED, DataSets.DAILY_MODE, executeDate)
    DataWriter.writeParquet(joinedData, writePath, saveMode)
    DataWriter.writeCsv(joinedData,DataSets.DCF_FEED,DataSets.CLICKSTREAM_MERGED_FEED,DataSets.DAILY_MODE, executeDate, DataSets.DCF_FEED_FILENAME+changedDateFormat, DataSets.ERROR_SAVEMODE, "true", ";")
  }

  def convertFeedFormat(pageVisitData: DataFrame, deviceMapping: DataFrame): DataFrame = {
    val joinedData = pageVisitData.join(deviceMapping, pageVisitData("userid") === deviceMapping("email"), "inner")
      .select(
        deviceMapping("id_customer"),
        pageVisitData("productsku"),
        changeDateFormatValue(pageVisitData("pagets"), lit("yyyy-MM-dd HH:mm:ss.SSS"), lit("yyyy-MM-dd'T' HH:mm:ss'Z'")) as "pagets",
        pageVisitData("sessionid"),
        pageVisitData("productsku")
      )
    return joinedData
  }

  val changeDateFormatValue = udf((date: Timestamp, initialFormat: String, expectedFormat: String) => TimeUtils.changeDateFormat(date: Timestamp, initialFormat: String, expectedFormat: String))

}
