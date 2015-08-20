package com.jabong.dap.export.dcf

import java.sql.Timestamp

import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by rahul on 20/8/15.
 */
object DcfFeedGenerator extends Logging {
  private var sc:SparkContext = null
  private var hiveContext:HiveContext = null
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("clickStreamDCFFeed")
    sc = new SparkContext(conf)
    hiveContext = new HiveContext(sc)
    if(args.length < 1){
      println("Please provide correct arguments :output:- \t  date:- yyyy/mm/dd by default yesterday \t table:- by default merge.merge_pagevisit")
      System.exit(0)
    }
    var executeDate:String = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
    var clickstreamTable = "merge.merge_pagevisit"
    val outPath = args(0)
    if(args(1) != null) executeDate = args(1)
    if(args(2) != null) clickstreamTable = args(2)
    val monthYear = TimeUtils.getMonthAndYear(executeDate,TimeConstants.DATE_FORMAT_FOLDER)
    val month = monthYear.month + 1
    val date = monthYear.day
    val year = monthYear.year
    val dateFolder = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)

    val cmr = DataReader.getDataFrame(ConfigConstants.OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, executeDate)
//    val monthWithZero = withLeadingZeros(month)
//    val dateWithZero = withLeadingZeros(date)
//    println("/data/output/extras/device_mapping/full/"+year+"/"+monthWithZero+"/"+dateWithZero)
//    val deviceMapping = hiveContext.read.parquet("/data/output/extras/device_mapping/full/"+year+"/"+monthWithZero+"/"+dateWithZero+"/*/")
    println("deviceMapping"+cmr.count())

    println("SELECT userid, productsku,pagets,sessionid FROM "+clickstreamTable +
      " where pagetype in ('CPD','QPD','DPD') and date1 = " +date+" and month1 = "+month+" and year1="+year)

    val pageVisitData = hiveContext.sql("SELECT userid, productsku,pagets,sessionid FROM "+ clickstreamTable +
      " where pagetype in ('CPD','QPD','DPD') and userid is not null and date1 = " +date+" and month1 = "+month+" and year1="+year)

    println("pageVisitCount"+pageVisitData.count)

    val joinedData = convertFeedFormat(pageVisitData,outPath,cmr)
    val changedDateFormat = TimeUtils.changeDateFormat(executeDate,TimeConstants.DATE_FORMAT_FOLDER,TimeConstants.DATE_FORMAT)
    val writePath =  DataWriter.getWritePath(args(0),DataSets.DCF_FEED,DataSets.CLICKSTREAM_MERGED_FEED,DataSets.DAILY_MODE,executeDate)
    DataWriter.writeParquet(joinedData,writePath,DataSets.ERROR_SAVEMODE)
  //  DataWriter.writeCsv(joinedData,DataSets.DCF_FEED,DataSets.CLICKSTREAM_MERGED_FEED,DataSets.DAILY_MODE, executeDate, DataSets.DCF_FEED_FILENAME+changedDateFormat, DataSets.ERROR_SAVEMODE, "true", ";")
  }


  def convertFeedFormat(pageVisitData:DataFrame, path: String,deviceMapping:DataFrame): DataFrame ={
    val joinedData = pageVisitData.join(deviceMapping,pageVisitData("userid")===deviceMapping("email"),"inner")
      .select(
        deviceMapping("id_customer"),
        pageVisitData("productsku"),
        changeDateFormatValue(pageVisitData("pagets"),lit("yyyy-MM-dd HH:mm:ss.SSS"),lit("yyyy-MM-dd'T' HH:mm:ss'Z'")) as "pagets",
        pageVisitData("sessionid"),
        pageVisitData("productsku")
      )
    joinedData.write.parquet(path)
    return joinedData
  }

  val changeDateFormatValue = udf((date: Timestamp , initialFormat: String, expectedFormat: String) => TimeUtils.changeDateFormat(date:Timestamp, initialFormat: String, expectedFormat: String))


}
