package com.jabong.dap.quality.Clickstream

import com.jabong.dap.common.mail.ScalaMail
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.{ OptionUtils, Spark }
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.model.clickstream.ClickStreamConstant
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by tejas on 13/8/15.
 */
object DataQualityMethods extends Logging {

  def start(params: ParamInfo): Unit = {

    logger.info("clickstreamDataQualityCheck started")
    val hiveContext = Spark.getHiveContext()
    val executeDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val saveMode = params.saveMode
    //val clickstreamTable = OptionUtils.getOptValue(params.input, DataSets.DCF_INPUT_MERGED_HIVE_TABLE)
    val monthYear = TimeUtils.getMonthAndYear(executeDate, TimeConstants.DATE_FORMAT_FOLDER)
    val month = (monthYear.month + 1).toString
    val date = monthYear.day.toString
    val year = monthYear.year.toString

    val output = DataQualityMethods.Artemisdaily(hiveContext, date, month, year, ClickStreamConstant.CLICKSTREAM_ARTEMIS_TABLE, ClickStreamConstant.CLICKSTREAM_APPS_TABLE, ClickStreamConstant.CLICKSTREAM_DESKTOP_TABLE, ClickStreamConstant.MERGE_PAGEVISIT)
    //var path= "./"+year+"/"+month+"/"+date
    //logger.info("Value of output path"+ConfigConstants.WRITE_OUTPUT_PATH)
    //val finaloutput = Spark.getContext().parallelize(output)
    //finaloutput.coalesce(1,true).saveAsTextFile(PathBuilder.buildPath(ConfigConstants.WRITE_OUTPUT_PATH, ClickStreamConstant.CLICKSTREAM_DATA_QUALITY, "CLICKSTREAM_QUALITY", DataSets.DAILY_MODE, date))
    //write(ConfigConstants.OUTPUT_PATH, "./Automation1/",output.getBytes())
    ScalaMail.sendMessage("tech.dap@jabong.com", "", "", "tech.dap@jabong.com", "Quality Report", output, "")
    //println("ScalaMailMain")

    ScalaMail.sendMessage("tech.dap@jabong.com", "", "", "tech.dap@jabong.com", "Quality Report", output, "")

  }
  def Artemisdaily(hiveContext: HiveContext, day: String, month: String, year: String, clickStreamArtemisTable: String, clickStreamAppsTable: String, clickStreamDesktopTable: String, clickStreamMergeTable: String): String = {

    val data = hiveContext.sql("select id, bid, visitid, pagets, actualvisitid, channel, ip, url, pagetype, domain, device, useragent, year1, month1, date1 from " + clickStreamArtemisTable + " where date1 = " + day + " and month1 = " + month + " and year1 = " + year).persist()

    val clickstreamapps = hiveContext.sql("select id, bid, visitid, pagets, actualvisitid, channel, ip, url, pagetype, domain, device, useragent, year1, month1, date1 from " + clickStreamAppsTable + "  where date1 = " + day + " and month1 = " + month + " and year1 = " + year).persist()

    val clickstreampagevisit = hiveContext.sql("select id, browserid as bid, visitid, pagets, actualvisitid, channel, ip, url, pagetype, domain, device, useragent, year1, month1, date1 from " + clickStreamDesktopTable + "  where date1 = " + day + " and month1 = " + month + " and year1 = " + year).persist()

    val mergepagevisit = hiveContext.sql("select id, browserid as bid, visitid, pagets, actualvisitid, channel, ip, url, pagetype, domain, device, useragent, year1, month1, date1 from " + clickStreamMergeTable + " where date1 = " + day + " and month1 = " + month + " and year1 = " + year).persist()

    var msg = qualityCount(data, "Clickstream_Artemis_Desktop", "")
    msg = qualityCount(clickstreamapps, "Clickstream_apps", msg)
    msg = qualityCount(clickstreampagevisit, "Clickstream_Desktop_pagevisit", msg)
    msg = qualityCount(mergepagevisit, "merge_pagevisit", msg)
    return msg
  }

  def qualityCount(resultSet: DataFrame, processName: String, oldMessage: String): String = {
    var message: String = oldMessage
    val datacount = resultSet.count()
    if (datacount == 0) {
      message += "There is no data available in " + processName
    } else {

      message += processName + " details:" + "\n \n"

      message += "Count of records :" + datacount.toString() + "\n"

      message += "Count of Null ID :" + resultSet.filter(resultSet("id") isNull).count() + "\n"

      message += "Count of Null browserid :" + resultSet.filter(resultSet("bid") isNull).count() + "\n"

      message += "Count of Null visitid :" + resultSet.filter(resultSet("visitid") isNull).count() + "\n"

      message += "Count of Null pagets :" + resultSet.filter(resultSet("pagets") isNull).count() + "\n"

      message += "Count of Null actualvisitid :" + resultSet.filter(resultSet("actualvisitid") isNull).count() + "\n"

      message += "Count of Null channel :" + resultSet.filter(resultSet("channel") isNull).count() + "\n"

      message += "Count of Null ip :" + resultSet.filter(resultSet("ip") isNull).count() + "\n"

      message += "Count of Null url :" + resultSet.filter(resultSet("url") isNull).count() + "\n"

      message += "Count of Null pagetype :" + resultSet.filter(resultSet("pagetype") isNull).count() + "\n"

      message += "Count of Null domain :" + resultSet.filter(resultSet("domain") isNull).count() + "\n"

      message += "Count of m domain :" + resultSet.filter(resultSet("domain") equalTo ("m")).count() + "\n"

      message += "Count of w domain :" + resultSet.filter(resultSet("domain") equalTo ("w")).count() + "\n"

      message += "Count of windows domain :" + resultSet.filter(resultSet("domain") equalTo ("windows")).count() + "\n"

      message += "Count of android domain :" + resultSet.filter(resultSet("domain") equalTo ("android")).count() + "\n"

      message += "Count of ios domain :" + resultSet.filter(resultSet("domain") equalTo ("ios")).count() + "\n"

      message += "Count of Null device :" + resultSet.filter(resultSet("device") isNull).count() + "\n"

      message += "Count of Mobile device :" + resultSet.filter(resultSet("device") equalTo ("mobile")).count() + "\n"

      message += "Count of Tablet device :" + resultSet.filter(resultSet("device") equalTo ("tablet")).count() + "\n"

      message += "Count of desktop device :" + resultSet.filter(resultSet("device") equalTo ("desktop")).count() + "\n"

      message += "Count of Null useragent :" + resultSet.filter(resultSet("useragent") isNull).count() + "\n"

      message += "Count of Null year1 :" + resultSet.filter(resultSet("year1") isNull).count() + "\n"

      message += "Count of Null month1 :" + resultSet.filter(resultSet("month1") isNull).count() + "\n"

      message += "Count of Null date1 :" + resultSet.filter(resultSet("date1") isNull).count() + "\n \n"

    }

    return message
  }
}
