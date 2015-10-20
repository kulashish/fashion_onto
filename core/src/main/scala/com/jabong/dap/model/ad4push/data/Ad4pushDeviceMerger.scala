package com.jabong.dap.model.ad4push.data

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.Ad4pushVariables
import com.jabong.dap.common.schema.SchemaUtils
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.ad4push.schema.Ad4pushSchema
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by mubarak on 21/8/15.
 */
object Ad4pushDeviceMerger extends Logging {

  def start(params: ParamInfo, isHistory: Boolean) = {
    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val saveMode = params.saveMode
    val path = OptionUtils.getOptValue(params.path)
    var paths: Array[String] = new Array[String](2)
    if (null != path) {
      paths = path.split(";")
    }
    val prevDate = OptionUtils.getOptValue(params.fullDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER, incrDate))

    if (isHistory && null == path && null == OptionUtils.getOptValue(params.fullDate)) {
      logger.error("First full csv path and prev full date both cannot be empty")
    } else {
      val newDate = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)

      val filenameIos = "exportDevices_" + DataSets.IOS_CODE + "_" + newDate
      processData(DataSets.DEVICES_IOS, prevDate, incrDate, filenameIos, saveMode, DataSets.IOS, paths(0))

      val filenameAndroid = "exportDevices_" + DataSets.ANDROID_CODE + "_" + newDate
      processData(DataSets.DEVICES_ANDROID, prevDate, incrDate, filenameAndroid, saveMode, DataSets.ANDROID, paths(1))
    }

    if (isHistory) {
      processHistoricalData(incrDate, saveMode)
    }
  }

  /**
   *
   * @param prevDate
   * @param fullcsv
   * @param curDate
   */
  def processData(tablename: String, prevDate: String, curDate: String, filename: String, saveMode: String, deviceType: String, fullcsv: String) {
    // Using Select Coalesce function
    var newDF: DataFrame = null
    newDF = DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.AD4PUSH, tablename, DataSets.DAILY_MODE, curDate, filename + ".csv", "true", ",")
      .dropDuplicates()
    if (deviceType.equalsIgnoreCase(DataSets.ANDROID)) {
      if (!SchemaUtils.isSchemaEqual(newDF.schema, Ad4pushSchema.Ad4pushDeviceIOS)) {
        newDF = SchemaUtils.changeSchema(newDF, Ad4pushSchema.Ad4pushDeviceIOS)
      }
    }

    var full: DataFrame = null
    if (null != fullcsv) {
      full = DataReader.getDataFrame4mCsv(fullcsv, "true", ";").withColumnRenamed(Ad4pushVariables.DEVICE_ID, Ad4pushVariables.UDID)
      if (!SchemaUtils.isSchemaEqual(full.schema, Ad4pushSchema.Ad4pushDeviceIOS)) {
        full = SchemaUtils.changeSchema(full, Ad4pushSchema.Ad4pushDeviceIOS)
        full = SchemaUtils.dropColumns(full, Ad4pushSchema.Ad4pushDeviceIOS).dropDuplicates()
      }
    } else {
      full = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.AD4PUSH, tablename, DataSets.FULL_MERGE_MODE, prevDate)
    }

    val res = mergeExportData(full, newDF).dropDuplicates()
    val savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.AD4PUSH, tablename, DataSets.FULL_MERGE_MODE, curDate)
    if (DataWriter.canWrite(saveMode, savePath))
      DataWriter.writeParquet(res, savePath, saveMode)
    if (deviceType.equalsIgnoreCase(DataSets.ANDROID)) {
      DataWriter.writeCsv(SchemaUtils.dropColumns(res, Ad4pushSchema.Ad4pushDeviceAndroid), DataSets.AD4PUSH, tablename, DataSets.FULL_MERGE_MODE, curDate, filename, saveMode, "true", ";")
    } else {
      DataWriter.writeCsv(res, DataSets.AD4PUSH, tablename, DataSets.FULL_MERGE_MODE, curDate, filename, saveMode, "true", ";")
    }
  }

  def mergeExportData(full: DataFrame, newdf: DataFrame): DataFrame = {

    val joined = full.join(newdf, full(Ad4pushVariables.UDID) === newdf(Ad4pushVariables.UDID), SQL.FULL_OUTER)
      .select(coalesce(full(Ad4pushVariables.UDID), newdf(Ad4pushVariables.UDID)) as Ad4pushVariables.UDID,
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
        coalesce(newdf(Ad4pushVariables.LOGIN_USER_ID), full(Ad4pushVariables.LOGIN_USER_ID)) as Ad4pushVariables.LOGIN_USER_ID,
        coalesce(newdf(Ad4pushVariables.MOST_VISITED_CATEGORY), full(Ad4pushVariables.MOST_VISITED_CATEGORY)) as Ad4pushVariables.MOST_VISITED_CATEGORY,
        coalesce(newdf(Ad4pushVariables.ORDER_STATUS), full(Ad4pushVariables.ORDER_STATUS)) as Ad4pushVariables.ORDER_STATUS,
        coalesce(newdf(Ad4pushVariables.PURCHASE), full(Ad4pushVariables.PURCHASE)) as Ad4pushVariables.PURCHASE,
        coalesce(newdf(Ad4pushVariables.REGISTRATION), full(Ad4pushVariables.REGISTRATION)) as Ad4pushVariables.REGISTRATION,
        coalesce(newdf(Ad4pushVariables.STATUS_IN_APP), full(Ad4pushVariables.STATUS_IN_APP)) as Ad4pushVariables.STATUS_IN_APP,
        coalesce(newdf(Ad4pushVariables.WISHLIST_STATUS), full(Ad4pushVariables.WISHLIST_STATUS)) as Ad4pushVariables.WISHLIST_STATUS,
        coalesce(newdf(Ad4pushVariables.WISHLIST_ADD), full(Ad4pushVariables.WISHLIST_ADD)) as Ad4pushVariables.WISHLIST_ADD,
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
    joined
  }

  def processHistoricalData(minDate: String, saveMode: String) {
    println("Inside Historical Data merge Code")

    val noOfDays = TimeUtils.daysFromToday(minDate, TimeConstants.DATE_FORMAT_FOLDER) - 1
    var prevFullDate: String = null
    var incrDate: String = minDate
    for (i <- 1 to noOfDays) {
      prevFullDate = incrDate
      incrDate = TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, prevFullDate)
      val newDate = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)

      println("Increment Date: " + incrDate)
      println("prevDate: " + prevFullDate)
      //exportDevices_517_20150822.csv
      val filenameIos = "exportDevices_" + DataSets.IOS_CODE + "_" + newDate
      processData(DataSets.DEVICES_IOS, prevFullDate, incrDate, filenameIos, saveMode, DataSets.IOS, null)
      val filenameAndroid = "exportDevices_" + DataSets.ANDROID_CODE + "_" + newDate
      processData(DataSets.DEVICES_ANDROID, prevFullDate, incrDate, filenameAndroid, saveMode, DataSets.ANDROID, null)

      println("successfully done merge")

    }
  }

}
