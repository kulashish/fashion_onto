package com.jabong.dap.quality.Clickstream

import java.util.Calendar

import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.common.{OptionUtils, Spark}
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.storage.DataSets
import grizzled.slf4j.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by tejas on 13/8/15.
 */
object DataQualityMethods extends Logging {


  def start(params: ParamInfo): Unit = {

    logger.info("dcf feed generation process started")
    val hiveContext = Spark.getHiveContext()
    val executeDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val saveMode = params.saveMode
    val clickstreamTable = OptionUtils.getOptValue(params.input, DataSets.DCF_INPUT_MERGED_HIVE_TABLE)
    val monthYear = TimeUtils.getMonthAndYear(executeDate, TimeConstants.DATE_FORMAT_FOLDER)
    val month = (monthYear.month + 1).toString
    val date = monthYear.day.toString
    val year = monthYear.year.toString

    //Spark.getContext()
    //val conf = new SparkConf().setAppName("Clickstream Surf Variables").set("spark.driver.allowMultipleContexts", "true")
    //Spark.init(conf)

    //val hiveContext = Spark.getHiveContext()
    //val sqlContext = Spark.getSqlContext()
    //var gap = args(0).toInt
    var tablename = DataSets.CLICKSTREAM_DESKTOP_TABLE
    var tablename1 = DataSets.CLICKSTREAM_APPS_TABLE
    var tablename2= DataSets.CLICKSTREAM_PAGEVISIT_TABLE
    var tablename3 = DataSets.MERGE_PAGEVISIT
    val cal = Calendar.getInstance()
    //cal.add(Calendar.DATE, -gap)
    //val mFormat = new SimpleDateFormat("MM")
    //var year = cal.get(Calendar.YEAR).toString
    //var day = cal.get(Calendar.DAY_OF_MONTH).toString
    //var month = mFormat.format(cal.getTime())
    //val dateFormat = new SimpleDateFormat("dd/MM/YYYY")
    //var dt = dateFormat.format(cal.getTime())
    //var res1 = DataQualityMethods.Artemisdaily(hiveContext, day, month, year, tablename)

    val output = DataQualityMethods.Artemisdaily(hiveContext, date, month, year, tablename, tablename1, tablename2,tablename3)
    var path= "./"+year+"/"+month+"/"+date
    write("hdfs://172.16.84.37:8020", path,output.getBytes())
    //write(ConfigConstants.OUTPUT_PATH, "./Automation1/",output.getBytes())
    //ScalaMail.sendMessage("tejas.jain@jabong.com","","","tejas.jain@jabong.com",output,"Quality Report","")

    //println("ScalaMailMain")
  }

  def write(uri: String, filePath: String,data: Array[Byte]) = {
    System.setProperty("HADOOP_USER_NAME", "tjain")
    val path = new Path(filePath)
    val conf = new Configuration()
    conf.set("fs.defaultFS", uri)
    val fs = FileSystem.get(conf)
    val os = fs.create(path)
    os.write(data)
    fs.close()
  }

def Artemisdaily(hiveContext: HiveContext, day: String, month: String, year: String, tablename: String, tablename1: String, tablename2: String, tablename3: String): String = {


    val data = hiveContext.sql("select id, bid, visitid, pagets, actualvisitid, channel, ip, url, pagetype, domain, device, useragent, year1, month1, date1 from " + tablename + " where date1 = " + day + " and month1 = " + month + " and year1 = " + year).persist()

    val clickstreamapps = hiveContext.sql("select id, bid, visitid, pagets, actualvisitid, channel, ip, url, pagetype, domain, device, useragent, year1, month1, date1 from " + tablename1 + "  where date1 = " + day + " and month1 = " + month + " and year1 = " + year).persist()

    val clickstreampagevisit = hiveContext.sql("select id, browserid as bid, visitid, pagets, actualvisitid, channel, ip, url, pagetype, domain, device, useragent, year1, month1, date1 from " + tablename2 + "  where date1 = " + day + " and month1 = " + month + " and year1 = " + year).persist()

    val mergepagevisit = hiveContext.sql("select id, browserid as bid, visitid, pagets, actualvisitid, channel, ip, url, pagetype, domain, device, useragent, year1, month1, date1 from " + tablename3 + " where date1 = " + day + " and month1 = " + month + " and year1 = " + year).persist()

    //val datacount = hiveContext.sql("select count(*) from " + tablename + " where date1 = " + day + " and month1 = " + month + " and year1 = " + year)

    //val a :Long= datacount.collect()(0).getLong(0)

    /** ************************************************************************************************/
    //clickstream_desktop//
    /** ************************************************************************************************/
    var msg = qualityCount(data, "Clickstream_Desktop", "")
    msg = qualityCount(clickstreamapps, "Clickstream_apps", msg)
    msg = qualityCount(clickstreampagevisit, "Clickstream_pagevisit", msg)
    msg = qualityCount(mergepagevisit, "merge_pagevisit", msg)
    return msg
  }

  def qualityCount(resultSet: DataFrame, processName: String, oldMessage: String): String = {
    var message: String = oldMessage
    val datacount = resultSet.count()
    if (datacount == 0) {
      message += "There is no data available in " + processName
    }
    else {

      message += processName + "details" + "\n"

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

      message += "Count of Null date1 :" + resultSet.filter(resultSet("date1") isNull).count() + "\n"

    }

    return message

  }
}
