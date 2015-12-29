package com.jabong.dap.adjust

import java.net.URLDecoder
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{ HashPartitioner, SparkConf, SparkContext }
import org.apache.spark.{ SparkConf, HashPartitioner, SparkContext }
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

/**
 * Created by harbinder on 19/6/15.
 * Formats raw data present in input directory path cli argument
 * Creates different file /partition for each event
 */

class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get()

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
    key.asInstanceOf[String]
}

object CsvCreator {

  val headerString = Array("event", "?os", "environment", "user_id", "campaign", "adid", "google_play_advertising_id", "store", "reftag", "adgroup", "creative",
    "transaction_id", "payment_option_selected", "revenue", "price", "sku", "skus",
    "ip", "country", "shop_country", "currency_code", "os_version", "network", "network_class",
    "device", "DeviceID", "device_id", "wp_device_id", "device_model", "device_name", "device_manufacturer", "display_size",
    "bundle", "app_version", "sdk_version", "user_agent", "session", "language", "tracking_enabled",
    "time", "install_time", "Date", "time_spent", "timezone", "click_time", "duration", "last_time_spent",
    "fb_campaign_name", "fb_campaign_id", "fb_adset_name", "fb_adset_id", "fb_ad_name", "fb_ad_id"
  )
  val Headers = headerString

  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Please specify all arguments:- InputDirectory  OutputDirectory")
      sys.exit(0)
    }

    val conf = new SparkConf().setAppName("Adjust Log Csv")
    val sc = new SparkContext(conf)

    //TODO: merge all replace in one
    val lineArr = sc.textFile(args(0))
      .map(line => line.replace('&', ',')
	.replace("=event", "=XXX")
        .replace('=', ',')
        .replace("/v1/events", ",")
        .replace("transaction_ID", "transaction_id")
        .replace("order_value", "price")
        .split(",")
      )
      .map(lineArr => ArrtoSortedRow(lineArr))
      .filter(row => !row.isEmpty)
      .map(row => (row.substring(0, row.indexOf("eEND,")), row.substring(row.indexOf("eEND,") + 5, row.length)))
      .partitionBy(new HashPartitioner(50))
      .saveAsHadoopFile(args(1), classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])
  }

  def ArrtoSortedRow(lineArr: Array[String]): String = {

    var sortedRow = "" //headerString.toString
    for (col <- Headers) {
      if (lineArr(lineArr.indexOf(col).toInt + 1).isEmpty) {
        sortedRow = sortedRow + "null" + ","
      } else {
        if (col == "event") {
          sortedRow = sortedRow + lineArr(lineArr.indexOf(col).toInt + 1).toString + "eEND,"
        } else {
          sortedRow = sortedRow + URLDecoder.decode(lineArr(lineArr.indexOf(col).toInt + 1).toString, "UTF-8").replace(",", "|").toString + ","
        }
      }
    }
    return sortedRow.substring(0, sortedRow.length - 1)
  }
}
