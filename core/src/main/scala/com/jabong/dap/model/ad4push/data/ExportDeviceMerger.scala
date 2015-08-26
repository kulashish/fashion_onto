package com.jabong.dap.model.ad4push.data

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{Ad4pushVariables}
import com.jabong.dap.common.schema.SchemaUtils
import com.jabong.dap.data.read.{ValidFormatNotFound, DataNotFound, DataReader}
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.MergeUtils
import com.jabong.dap.data.write.DataWriter
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import grizzled.slf4j.Logging
import org.apache.spark.sql.functions._
import com.jabong.dap.model.ad4push.schema.DevicesReactionsSchema


/**
 * Created by mubarak on 21/8/15.
 */
object ExportDeviceMerger extends Logging   {



  /**
   *
   * @param prevDate
   * @param fullcsv
   * @param curDate
   */
  def processData(prevDate: String, fullcsv: String, curDate: String, saveMode: String, deviceType: String) {
    val filename = ".csv"
    var newDF: DataFrame = null
    newDF = DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.AD4PUSH, DataSets.EXPORT_DEVICE+deviceType, DataSets.DAILY_MODE, curDate, filename, "true", ",")
    if (deviceType.equalsIgnoreCase("517")) {
      if (deviceType.equalsIgnoreCase("517")) {
        newDF = SchemaUtils.changeSchema(newDF, DevicesReactionsSchema.Ad4pushDevice515)
      }
    }

    var full: DataFrame = null
    if (null != fullcsv) {
      full = DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.AD4PUSH, DataSets.EXPORT_DEVICE+deviceType, DataSets.DAILY_MODE, curDate, fullcsv, "true", ",")
      if (deviceType.equalsIgnoreCase("517")) {
          full = SchemaUtils.changeSchema(full, DevicesReactionsSchema.Ad4pushDevice515)
      }
    } else {
      full = DataReader.getDataFrame(ConfigConstants.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.EXPORT_DEVICE+deviceType, DataSets.FULL_MERGE_MODE, prevDate)
    }

    val res = mergeExportData(full, newDF)
    val savePath = DataWriter.getWritePath(ConfigConstants.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.EXPORT_DEVICE+deviceType, DataSets.FULL_MERGE_MODE, curDate)
    if (DataWriter.canWrite(saveMode, savePath))
      DataWriter.writeParquet(res, savePath, saveMode)
    if (deviceType.equalsIgnoreCase("517")){
      DataWriter.writeCsv(SchemaUtils.dropColumns(res, DevicesReactionsSchema.Ad4pushDevice517), ConfigConstants.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.FULL_MERGE_MODE, curDate, DataSets.EXPORT_DEVICE+deviceType, DataSets.OVERWRITE_SAVEMODE, "true", ",")
    } else {
      DataWriter.writeCsv(res, ConfigConstants.OUTPUT_PATH, DataSets.AD4PUSH, DataSets.FULL_MERGE_MODE, curDate, DataSets.EXPORT_DEVICE+deviceType, DataSets.OVERWRITE_SAVEMODE, "true", ",")
    }
  }

  def readFromCSV(path: String): DataFrame={
    require(path != null, "Path is null")

    try {
      val df = Spark.getSqlContext().read.format("com.databricks.spark.csv").option("header", "true").
        option("delimiter", ",").
        load(path)
      println("Total recs in Export file initially: " + df.count())
      return df

    } catch {
      case e: DataNotFound =>
        logger.error("Data not found for the given path ")
        throw new DataNotFound
      case e: ValidFormatNotFound =>
        logger.error("Format could not be resolved for the given files in directory")
        throw new ValidFormatNotFound
    }
  }


  def mergeExportData(full: DataFrame, newdf: DataFrame): DataFrame = {

    val joined = full.join(newdf, full(Ad4pushVariables.UDID) === newdf(Ad4pushVariables.UDID), "outer")
                      .select(coalesce(full(Ad4pushVariables.UDID),newdf(Ad4pushVariables.UDID)) as Ad4pushVariables.UDID,
        coalesce(newdf(Ad4pushVariables.TOKEN), full(Ad4pushVariables.TOKEN)) as Ad4pushVariables.TOKEN,
        coalesce(newdf(Ad4pushVariables.OPENCOUNT), full(Ad4pushVariables.OPENCOUNT)) as Ad4pushVariables.OPENCOUNT,
        coalesce(newdf(Ad4pushVariables.FIRSTOPEN), full(Ad4pushVariables.FIRSTOPEN)) as Ad4pushVariables.FIRSTOPEN,
        coalesce(newdf(Ad4pushVariables.LASTOPEN), full(Ad4pushVariables.LASTOPEN)) as Ad4pushVariables.LASTOPEN,
        coalesce(newdf(Ad4pushVariables.MODEL), full(Ad4pushVariables.MODEL)) as Ad4pushVariables.MODEL,
        coalesce(newdf(Ad4pushVariables.VERSION), full(Ad4pushVariables.VERSION)) as Ad4pushVariables.VERSION,
        coalesce(newdf(Ad4pushVariables.LANGUAGE), full(Ad4pushVariables.LANGUAGE)) as Ad4pushVariables.LANGUAGE,
        coalesce(newdf(Ad4pushVariables.BUNDLEVERSION), full(Ad4pushVariables.BUNDLEVERSION)) as Ad4pushVariables.BUNDLEVERSION,
        coalesce(newdf(Ad4pushVariables.LAT), full(Ad4pushVariables.LAT)) as Ad4pushVariables.LAT,
        coalesce(newdf(Ad4pushVariables.LON), full(Ad4pushVariables.LON)) as Ad4pushVariables.LON,
        coalesce(newdf(Ad4pushVariables.ALTITUDE), full(Ad4pushVariables.ALTITUDE)) as Ad4pushVariables.ALTITUDE,
        coalesce(newdf(Ad4pushVariables.GEOLOCATION_CREATED), full(Ad4pushVariables.GEOLOCATION_CREATED)) as Ad4pushVariables.GEOLOCATION_CREATED,
        coalesce(newdf(Ad4pushVariables.VERSIONSDK), full(Ad4pushVariables.VERSIONSDK)) as Ad4pushVariables.VERSIONSDK,
        coalesce(newdf(Ad4pushVariables.FEEDBACK), full(Ad4pushVariables.FEEDBACK)) as Ad4pushVariables.FEEDBACK,
        coalesce(newdf(Ad4pushVariables.TIME_ZONE), full(Ad4pushVariables.TIME_ZONE)) as Ad4pushVariables.TIME_ZONE,
        coalesce(newdf(Ad4pushVariables.SYSTEM_OPTIN_NOTIFS), full(Ad4pushVariables.SYSTEM_OPTIN_NOTIFS)) as Ad4pushVariables.SYSTEM_OPTIN_NOTIFS,
        coalesce(newdf(Ad4pushVariables.ENABLED_NOTIFS), full(Ad4pushVariables.ENABLED_NOTIFS)) as Ad4pushVariables.ENABLED_NOTIFS,
        coalesce(newdf(Ad4pushVariables.ENABLED_INAPPS), full(Ad4pushVariables.ENABLED_INAPPS)) as Ad4pushVariables.ENABLED_INAPPS,
        coalesce(newdf(Ad4pushVariables.RANDOMID), full(Ad4pushVariables.RANDOMID)) as Ad4pushVariables.RANDOMID,
        coalesce(newdf(Ad4pushVariables.COUNTRYCODE), full(Ad4pushVariables.COUNTRYCODE)) as Ad4pushVariables.COUNTRYCODE,
        coalesce(newdf(Ad4pushVariables.AGGREGATED_NUMBER_OF_PURCHASES), full(Ad4pushVariables.AGGREGATED_NUMBER_OF_PURCHASES)) as Ad4pushVariables.AGGREGATED_NUMBER_OF_PURCHASES,
        coalesce(newdf(Ad4pushVariables.GENDER), full(Ad4pushVariables.GENDER)) as Ad4pushVariables.GENDER,
        coalesce(newdf(Ad4pushVariables.HAS_SHARED_PRODUCT), full(Ad4pushVariables.HAS_SHARED_PRODUCT)) as Ad4pushVariables.HAS_SHARED_PRODUCT,
        coalesce(newdf(Ad4pushVariables.LAST_ABANDONED_CART_DATE), full(Ad4pushVariables.LAST_ABANDONED_CART_DATE)) as Ad4pushVariables.LAST_ABANDONED_CART_DATE,
        coalesce(newdf(Ad4pushVariables.LAST_ABANDONED_CART_PRODUCT), full(Ad4pushVariables.LAST_ABANDONED_CART_PRODUCT)) as Ad4pushVariables.LAST_ABANDONED_CART_PRODUCT,
        coalesce(newdf(Ad4pushVariables.LASTORDERDATE), full(Ad4pushVariables.LASTORDERDATE)) as Ad4pushVariables.LASTORDERDATE,
        coalesce(newdf(Ad4pushVariables.LAST_SEARCH), full(Ad4pushVariables.LAST_SEARCH)) as Ad4pushVariables.LAST_SEARCH,
        coalesce(newdf(Ad4pushVariables.LAST_SEARCH_DATE), full(Ad4pushVariables.LAST_SEARCH_DATE)) as Ad4pushVariables.LAST_SEARCH_DATE,
        coalesce(newdf(Ad4pushVariables.LEAD), full(Ad4pushVariables.LEAD)) as Ad4pushVariables.LEAD,
        coalesce(newdf(Ad4pushVariables.MOST_VISITED_CATEGORY), full(Ad4pushVariables.MOST_VISITED_CATEGORY)) as Ad4pushVariables.MOST_VISITED_CATEGORY,
        coalesce(newdf(Ad4pushVariables.ORDER_STATUS), full(Ad4pushVariables.ORDER_STATUS)) as Ad4pushVariables.ORDER_STATUS,
        coalesce(newdf(Ad4pushVariables.PURCHASE), full(Ad4pushVariables.PURCHASE)) as Ad4pushVariables.PURCHASE,
        coalesce(newdf(Ad4pushVariables.REGISTRATION), full(Ad4pushVariables.REGISTRATION)) as Ad4pushVariables.REGISTRATION,
        coalesce(newdf(Ad4pushVariables.STATUS_IN_APP), full(Ad4pushVariables.STATUS_IN_APP)) as Ad4pushVariables.STATUS_IN_APP,
        coalesce(newdf(Ad4pushVariables.WISHLIST_STATUS), full(Ad4pushVariables.WISHLIST_STATUS)) as Ad4pushVariables.WISHLIST_STATUS,
        coalesce(newdf(Ad4pushVariables.SHOP_COUNTRY), full(Ad4pushVariables.SHOP_COUNTRY)) as Ad4pushVariables.SHOP_COUNTRY,
        coalesce(newdf(Ad4pushVariables.AMOUNT_BASKET), full(Ad4pushVariables.AMOUNT_BASKET)) as Ad4pushVariables.AMOUNT_BASKET,
        coalesce(newdf(Ad4pushVariables.CART), full(Ad4pushVariables.CART)) as Ad4pushVariables.CART,
        coalesce(newdf(Ad4pushVariables.SPECIFIC_CATEGORY_VISIT_COUNT), full(Ad4pushVariables.SPECIFIC_CATEGORY_VISIT_COUNT)) as Ad4pushVariables.SPECIFIC_CATEGORY_VISIT_COUNT,
        coalesce(newdf(Ad4pushVariables.USER_NAME), full(Ad4pushVariables.USER_NAME)) as Ad4pushVariables.USER_NAME,
        coalesce(newdf(Ad4pushVariables.LAST_VIEWED_CATEGORY), full(Ad4pushVariables.LAST_VIEWED_CATEGORY)) as Ad4pushVariables.LAST_VIEWED_CATEGORY,
        coalesce(newdf(Ad4pushVariables.MAX_VISITED_CATEGORY), full(Ad4pushVariables.MAX_VISITED_CATEGORY)) as Ad4pushVariables.MAX_VISITED_CATEGORY,
        coalesce(newdf(Ad4pushVariables.MOST_VISITED_COUNTS), full(Ad4pushVariables.MOST_VISITED_COUNTS)) as Ad4pushVariables.MOST_VISITED_COUNTS,
        coalesce(newdf(Ad4pushVariables.SEARCH_DATE), full(Ad4pushVariables.SEARCH_DATE)) as Ad4pushVariables.SEARCH_DATE,
        coalesce(newdf(Ad4pushVariables.IDFA), full(Ad4pushVariables.IDFA)) as Ad4pushVariables.IDFA,
        coalesce(newdf(Ad4pushVariables.LAST_ORDER_DATE), full(Ad4pushVariables.LAST_ORDER_DATE)) as Ad4pushVariables.LAST_ORDER_DATE,
        coalesce(newdf(Ad4pushVariables.WISHLIST_PRODUCTS_COUNT), full(Ad4pushVariables.WISHLIST_PRODUCTS_COUNT)) as Ad4pushVariables.WISHLIST_PRODUCTS_COUNT,
        coalesce(newdf(Ad4pushVariables.RATED), full(Ad4pushVariables.RATED)) as Ad4pushVariables.RATED
      )
    return joined
  }


   def main(args: Array[String]) {
     val conf = new SparkConf().setAppName("SparkExamples")
     Spark.init(conf)
     val df1 = readFromCSV("/home/jabong/bobdata/devices/exportDevices_515_20150821.csv")
     val df2 = readFromCSV("/home/jabong/bobdata/devices/exportDevices_515_20150822.csv")
     df1.limit(10).coalesce(1).write.format(DataSets.JSON).json("/home/jabong/bobdata/devices/exportDevices_515_1")
     df2.limit(10).coalesce(1).write.format(DataSets.JSON).json("/home/jabong/bobdata/devices/exportDevices_515_2")

     val t0 = System.nanoTime()
     val merged = mergeExportData(df1.limit(10), df2.limit(10))

       merged.coalesce(1).write.format(DataSets.JSON).json("/home/jabong/bobdata/devices/merged")

   }

}
