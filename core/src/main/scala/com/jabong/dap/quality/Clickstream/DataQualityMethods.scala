package com.jabong.dap.quality.Clickstream

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
 * Created by tejas on 13/8/15.
 */
object DataQualityMethods {

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
   /* var datacount = data.count()


    if(datacount==0){
      message="There is no data available in "+processName
    }
    else {

      message = "clickstream_desktop details" + "\n"

      message += "Count of records :" + datacount.toString() + "\n"

      message += "Count of Null ID :" + data.filter(data("id") isNull).count() + "\n"

      message += "Count of Null bid :" + data.filter(data("bid") isNull).count() + "\n"

      message += "Count of Null visitid :" + data.filter(data("visitid") isNull).count() + "\n"

      message += "Count of Null pagets :" + data.filter(data("pagets") isNull).count() + "\n"

      message += "Count of Null actualvisitid :" + data.filter(data("actualvisitid") isNull).count() + "\n"

      message += "Count of Null channel :" + data.filter(data("channel") isNull).count() + "\n"

      message += "Count of Null ip :" + data.filter(data("ip") isNull).count() + "\n"

      message += "Count of Null url :" + data.filter(data("url") isNull).count() + "\n"

      message += "Count of Null pagetype :" + data.filter(data("pagetype") isNull).count() + "\n"

      message += "Count of Null domain :" + data.filter(data("domain") isNull).count() + "\n"

      message += "Count of Null device :" + data.filter(data("device") isNull).count() + "\n"

      message += "Count of Null useragent :" + data.filter(data("useragent") isNull).count() + "\n"

      message += "Count of Null year1 :" + data.filter(data("year1") isNull).count() + "\n"

      message += "Count of Null month1 :" + data.filter(data("month1") isNull).count() + "\n"

      message += "Count of Null date1 :" + data.filter(data("date1") isNull).count() + "\n"


      /** *************************************************************************************/
      //clickstream_apps//
      /** ************************************************************************************/


      val appsdatacount = clickstreamapps.count()


      if (appsdatacount == 0) {
        message = "There is no data available in clickstream_apps"
      }
      else {

        message += "clickstream_apps details" + "\n"

        message += "Count of records :" + appsdatacount.toString() + "\n"

        message += "Count of Null ID :" + clickstreamapps.filter(clickstreamapps("id") isNull).count() + "\n"

        message += "Count of Null bid :" + clickstreamapps.filter(clickstreamapps("bid") isNull).count() + "\n"

        message += "Count of Null visitid :" + clickstreamapps.filter(clickstreamapps("visitid") isNull).count() + "\n"

        message += "Count of Null pagets :" + clickstreamapps.filter(clickstreamapps("pagets") isNull).count() + "\n"

        message += "Count of Null actualvisitid :" + clickstreamapps.filter(clickstreamapps("actualvisitid") isNull).count() + "\n"

        message += "Count of Null channel :" + clickstreamapps.filter(clickstreamapps("channel") isNull).count() + "\n"

        message += "Count of Null ip :" + clickstreamapps.filter(clickstreamapps("ip") isNull).count() + "\n"

        message += "Count of Null url :" + clickstreamapps.filter(clickstreamapps("url") isNull).count() + "\n"

        message += "Count of Null pagetype :" + clickstreamapps.filter(clickstreamapps("pagetype") isNull).count() + "\n"

        message += "Count of Null domain :" + clickstreamapps.filter(clickstreamapps("domain") isNull).count() + "\n"

        message += "Count of Null device :" + clickstreamapps.filter(clickstreamapps("device") isNull).count() + "\n"

        message += "Count of Null useragent :" + clickstreamapps.filter(clickstreamapps("useragent") isNull).count() + "\n"

        message += "Count of Null year1 :" + clickstreamapps.filter(clickstreamapps("year1") isNull).count() + "\n"

        message += "Count of Null month1 :" + clickstreamapps.filter(clickstreamapps("month1") isNull).count() + "\n"

        message += "Count of Null date1 :" + clickstreamapps.filter(clickstreamapps("date1") isNull).count() + "\n"

      }

      /** *************************************************************************************/
      //clickstream_pagevisit//
      /** *************************************************************************************/


     val pgdatacount = clickstreampagevisit.count()


      if (pgdatacount == 0) {
        message = "There is no data available in clickstream_pagevisit"
      }
      else {

        message += "clickstream_pagevisit details" + "\n"

        message += "Count of records :" + pgdatacount.toString() + "\n"

        message += "Count of Null ID :" + clickstreampagevisit.filter(clickstreampagevisit("id") isNull).count() + "\n"

        message += "Count of Null browserid :" + clickstreampagevisit.filter(clickstreampagevisit("browserid") isNull).count() + "\n"

        message += "Count of Null visitid :" + clickstreampagevisit.filter(clickstreampagevisit("visitid") isNull).count() + "\n"

        message += "Count of Null pagets :" + clickstreampagevisit.filter(clickstreampagevisit("pagets") isNull).count() + "\n"

        message += "Count of Null actualvisitid :" + clickstreampagevisit.filter(clickstreampagevisit("actualvisitid") isNull).count() + "\n"

        message += "Count of Null channel :" + clickstreampagevisit.filter(clickstreampagevisit("channel") isNull).count() + "\n"

        message += "Count of Null ip :" + clickstreampagevisit.filter(clickstreampagevisit("ip") isNull).count() + "\n"

        message += "Count of Null url :" + clickstreampagevisit.filter(clickstreampagevisit("url") isNull).count() + "\n"

        message += "Count of Null pagetype :" + clickstreampagevisit.filter(clickstreampagevisit("pagetype") isNull).count() + "\n"

        message += "Count of Null domain :" + clickstreampagevisit.filter(clickstreampagevisit("domain") isNull).count() + "\n"

        message += "Count of Null device :" + clickstreampagevisit.filter(clickstreampagevisit("device") isNull).count() + "\n"

        message += "Count of Null useragent :" + clickstreampagevisit.filter(clickstreampagevisit("useragent") isNull).count() + "\n"

        message += "Count of Null year1 :" + clickstreampagevisit.filter(clickstreampagevisit("year1") isNull).count() + "\n"

        message += "Count of Null month1 :" + clickstreampagevisit.filter(clickstreampagevisit("month1") isNull).count() + "\n"

        message += "Count of Null date1 :" + clickstreampagevisit.filter(clickstreampagevisit("date1") isNull).count() + "\n"

      }


      /** ********************************************************************************/
      //merge_pagevisit//
      /** ********************************************************************************/

      val mergedatacount = mergepagevisit.count()


      if (mergedatacount == 0) {
        message = "There is no data available in merge_pagevisit"
      }
      else {

        message += "merge_pagevisit details" + "\n"

        message += "Count of records :" + mergedatacount.toString() + "\n"

        message += "Count of Null ID :" + mergepagevisit.filter(mergepagevisit("id") isNull).count() + "\n"

        message += "Count of Null browserid :" + mergepagevisit.filter(mergepagevisit("browserid") isNull).count() + "\n"

        message += "Count of Null visitid :" + mergepagevisit.filter(mergepagevisit("visitid") isNull).count() + "\n"

        message += "Count of Null pagets :" + mergepagevisit.filter(mergepagevisit("pagets") isNull).count() + "\n"

        message += "Count of Null actualvisitid :" + mergepagevisit.filter(mergepagevisit("actualvisitid") isNull).count() + "\n"

        message += "Count of Null channel :" + mergepagevisit.filter(mergepagevisit("channel") isNull).count() + "\n"

        message += "Count of Null ip :" + mergepagevisit.filter(mergepagevisit("ip") isNull).count() + "\n"

        message += "Count of Null url :" + mergepagevisit.filter(mergepagevisit("url") isNull).count() + "\n"

        message += "Count of Null pagetype :" + mergepagevisit.filter(mergepagevisit("pagetype") isNull).count() + "\n"

        message += "Count of Null domain :" + mergepagevisit.filter(mergepagevisit("domain") isNull).count() + "\n"

        message += "Count of m domain :" + mergepagevisit.filter(mergepagevisit("domain") equalTo("m")).count() + "\n"

        message += "Count of w domain :" + mergepagevisit.filter(mergepagevisit("domain") equalTo("w")).count() + "\n"

        message += "Count of windows domain :" + mergepagevisit.filter(mergepagevisit("domain") equalTo("windows")).count() + "\n"

        message += "Count of android domain :" + mergepagevisit.filter(mergepagevisit("domain") equalTo("android")).count() + "\n"

        message += "Count of ios domain :" + mergepagevisit.filter(mergepagevisit("domain") equalTo("ios")).count() + "\n"

        message += "Count of Null device :" + mergepagevisit.filter(mergepagevisit("device") isNull).count() + "\n"

        message += "Count of Mobile device :" + mergepagevisit.filter(mergepagevisit("device") equalTo("mobile")).count() + "\n"

        message += "Count of Tablet device :" + mergepagevisit.filter(mergepagevisit("device") equalTo("tablet")).count() + "\n"

        message += "Count of desktop device :" + mergepagevisit.filter(mergepagevisit("device") equalTo("desktop")).count() + "\n"

        message += "Count of Null useragent :" + mergepagevisit.filter(mergepagevisit("useragent") isNull).count() + "\n"

        message += "Count of Null year1 :" + mergepagevisit.filter(mergepagevisit("year1") isNull).count() + "\n"

        message += "Count of Null month1 :" + mergepagevisit.filter(mergepagevisit("month1") isNull).count() + "\n"

        message += "Count of Null date1 :" + mergepagevisit.filter(mergepagevisit("date1") isNull).count() + "\n"

      }

    }

    return message
  }

}*/