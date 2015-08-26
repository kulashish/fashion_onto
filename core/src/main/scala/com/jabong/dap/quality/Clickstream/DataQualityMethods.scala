package com.jabong.dap.quality.Clickstream

import org.apache.spark.sql.hive.HiveContext


/**
 * Created by tejas on 13/8/15.
 */



object DataQualityMethods {

  def Artemisdaily(hiveContext: HiveContext, day: String, month: String, year: String, tablename: String): String = {
    var message: String = ""

    val id = hiveContext.sql("select count(*) from " + tablename + " where id is NULL and date1=" + day + " and month1=" + month + " and year1=" + year)
    message = "Count of null id:" + (id.take(1))(0).toString() + "\n"

    val bid = hiveContext.sql("select count(*) from " + tablename + " where bid is NULL and date1 = " + day + " and month1 = " + month + " and year1 = " + year)
    message += "Count of null bid:" + (bid.take(1))(0).toString() + "\n"

    val visitid = hiveContext.sql("select count(*) from " + tablename + " where visitid is NULL and date1 = " + day + " and month1 = " + month + " and year1 = " + year)
    message += "Count of null visitid:" + (visitid.take(1))(0).toString() + "\n"

    val pagets = hiveContext.sql("select count(*) from " + tablename + " where pagets is NULL and date1 = " + day + " and month1 = " + month + " and year1 = " + year)
    message += "Count of null pagets:" + (pagets.take(1))(0).toString() + "\n"

    val actualvisitid = hiveContext.sql("select count(*) from " + tablename + " where actualvisitid is NULL and date1 = " + day + " and month1 = " + month + " and year1 = " + year)
    message += "Count of null actualvisitid:" + (actualvisitid.take(1))(0).toString() + "\n"

    val channel = hiveContext.sql("select count(*) from " + tablename + " where channel is NULL and date1 = " + day + " and month1 = " + month + " and year1 = " + year)
    message += "Count of null channel:" + (channel.take(1))(0).toString() + "\n"

    val ip = hiveContext.sql("select count(*) from " + tablename + " where ip is NULL and date1 = " + day + " and month1 = " + month + " and year1 = " + year)
    message += "Count of null ip:" + (ip.take(1))(0).toString() + "\n"

    val url = hiveContext.sql("select count(*) from " + tablename + " where url is NULL and date1 = " + day + " and month1 = " + month + " and year1 = " + year)
    message += "Count of null id:" + (url.take(1))(0).toString() + "\n"

    val pagetype = hiveContext.sql("select count(*) from " + tablename + " where pagetype is NULL and date1 = " + day + " and month1 = " + month + " and year1 = " + year)
    message += "Count of null url:" + (pagetype.take(1))(0).toString() + "\n"

    val domain = hiveContext.sql("select count(*) from " + tablename + " where domain is NULL and date1 = " + day + " and month1 = " + month + " and year1 = " + year)
    message += "Count of null domain:" + (domain.take(1))(0).toString() + "\n"

    val devices = hiveContext.sql("select count(*) from " + tablename + " where device is NULL and date1 = " + day + " and month1 = " + month + " and year1 = " + year)
    message += "Count of null devices:" + (devices.take(1))(0).toString() + "\n"

    /*val distinctdevice = hiveContext.sql("select distinct device from " + tablename + " where date1 = " + day + " and month1 = " + month + " and year1 = " + year)
    message += "Count of null distinctdevice:" + (distinctdevice.take(1))(0).toString() + "\n"*/

    val useragent = hiveContext.sql("select count(*) from " + tablename + " where useragent is NULL and date1 = " + day + " and month1 = " + month + " and year1 = " + year)
    message += "Count of null useragent:" + (useragent.take(1))(0).toString() + "\n"

    val year1 = hiveContext.sql("select count(*) from " + tablename + " where year1 is NULL and date1 = " + day + " and month1 = " + month + " and year1 = " + year)
    message += "Count of null year1:" + (year1.take(1))(0).toString() + "\n"

    val month1 = hiveContext.sql("select count(*) from " + tablename + " where month1 is NULL and date1 = " + day + " and month1 = " + month + " and year1 = " + year)
    message += "Count of null month1:" + (month1.take(1))(0).toString() + "\n"

    val date1 = hiveContext.sql("select count(*) from " + tablename + " where date1 is NULL and month1 = " + month + " and year1 = " + year)
    message += "Count of null date1:" + (date1.take(1))(0).toString() + "\n"



    //val distinctdomain = hiveContext.sql("select distinct domain from " + tablename + " where date1 = " + day + " and month1 = " + month + " and year1 = " + year)
    //message += "Count of null distinctdomain:" + (distinctdomain.take(1))(0).toString() + "\n"


    val countclickstreamdesktop = hiveContext.sql("select count(*) from " + tablename + "")
    message += "Count of countclickstreamdesktop:" + (countclickstreamdesktop.take(1))(0).toString() + "\n"

    return message

  }

}
