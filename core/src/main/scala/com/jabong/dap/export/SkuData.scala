package com.jabong.dap.export

import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar

import com.jabong.dap.common.{OptionUtils, Spark}
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import com.jabong.dap.common.constants.SkuDataConst._

/**
 * Created by Kapil.Rajak on 17/8/15.
 */
object SkuData {

  def start(params: ParamInfo) = {
    val date = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val saveMode = params.saveMode
    io(date, saveMode)
  }

  /**
   * Entry poib
   * @param date default=yesterday
   * @param saveMode
   */
  def io(date:String=TimeUtils.yesterday(TimeConstants.DATE_FORMAT_FOLDER),saveMode: String)={
    val rawDF = getRawData(date)
    val result = skuBasedProcess(rawDF, date)

    val filename = DataSets.SKU_DATA +"_"+ DataSets.PRICING+"_"+TimeUtils.changeDateFormat(date,TimeConstants.DATE_FORMAT_FOLDER,TimeConstants.YYYYMMDD)
    val writePath = DataWriter.getWritePath(ConfigConstants.OUTPUT_PATH,DataSets.SKU_DATA, DataSets.PRICING, DataSets.DAILY_MODE,date)
    if (DataWriter.canWrite(writePath, saveMode)) {
      DataWriter.writeParquet(result, writePath, saveMode)
    }
    DataWriter.writeCsv(result, DataSets.SKU_DATA, DataSets.PRICING, DataSets.DAILY_MODE, date, filename, saveMode, "true", ";")
  }

  def skuBasedProcess(df: DataFrame, date: String):DataFrame={
    val dateYYYYMMDD = TimeUtils.changeDateFormat(date,TimeConstants.DATE_FORMAT_FOLDER,TimeConstants.YYYYMMDD)

    df.groupBy(PRODUCTSKU).agg(sum((col(DOMAIN)===DataSets.DESKTOP || col(DOMAIN)===DataSets.WSOA).cast(IntegerType)) as PAGEVISIT_DESKTOP,
                                sum((col(DOMAIN)===DataSets.MOBILEWEB).cast(IntegerType)) as PAGEVISIT_MOBILEWEB,
                                sum((col(DOMAIN)===DataSets.IOS).cast(IntegerType)) as PAGEVISIT_IOS,
                                sum((col(DOMAIN)===DataSets.ANDROID).cast(IntegerType)) as PAGEVISIT_ANDROID,
                                sum((col(DOMAIN)===DataSets.WINDOWS).cast(IntegerType)) as PAGEVISIT_WINDOWS).
                            withColumn(DATE, lit(dateYYYYMMDD)).
                            select(DATE,PRODUCTSKU,PAGEVISIT_DESKTOP,PAGEVISIT_MOBILEWEB,PAGEVISIT_IOS,PAGEVISIT_ANDROID,PAGEVISIT_WINDOWS)
  }

  def getRawData(date:String):DataFrame={
    val format = new SimpleDateFormat(TimeConstants.DATE_FORMAT_FOLDER)
    val dateObject = format.parse(date)
    val cal = Calendar.getInstance()
    cal.setTime(dateObject)
    val year = cal.get(Calendar.YEAR) //yFormat.format(dateObject)
    val day = cal.get(Calendar.DAY_OF_MONTH)
    val month = cal.get(Calendar.MONTH) + 1 //month starts from 0

    val tableName = "merge.merge_pagevisit"
    Spark.getHiveContext().sql("select productsku, domain from " + tableName + " where pagetype in('CPD','DPD','QPD') and domain is not null and date1=" + day + " and month1=" + month + " and year1=" + year)
  }
}
