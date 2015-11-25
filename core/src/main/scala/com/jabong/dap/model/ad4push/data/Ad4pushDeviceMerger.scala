package com.jabong.dap.model.ad4push.data

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.CustomerVariables
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
        newDF = SchemaUtils.addColumns(newDF, Ad4pushSchema.Ad4pushDeviceIOS)
      }
    }

    var full: DataFrame = null
    if (null != fullcsv) {
      full = DataReader.getDataFrame4mCsv(fullcsv, "true", ";").withColumnRenamed(CustomerVariables.DEVICE_ID, CustomerVariables.UDID)
      if (!SchemaUtils.isSchemaEqual(full.schema, Ad4pushSchema.Ad4pushDeviceIOS)) {
        full = SchemaUtils.addColumns(full, Ad4pushSchema.Ad4pushDeviceIOS)
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
      DataWriter.writeCsv(SchemaUtils.dropColumns(res, Ad4pushSchema.Ad4pushDeviceAndroid), DataSets.AD4PUSH, tablename, DataSets.FULL_MERGE_MODE, curDate, filename, saveMode, "true", "\t", 5)
    } else {
      DataWriter.writeCsv(res, DataSets.AD4PUSH, tablename, DataSets.FULL_MERGE_MODE, curDate, filename, saveMode, "true", "\t", 5)
    }
  }

  def mergeExportData(full: DataFrame, newdf: DataFrame): DataFrame = {

    val joined = full.join(newdf, full(CustomerVariables.UDID) === newdf(CustomerVariables.UDID), SQL.FULL_OUTER)
      .select(coalesce(full(CustomerVariables.UDID), newdf(CustomerVariables.UDID)) as CustomerVariables.UDID,
        coalesce(newdf(CustomerVariables.TOKEN), full(CustomerVariables.TOKEN)) as CustomerVariables.TOKEN,
        coalesce(newdf(CustomerVariables.OPENCOUNT), full(CustomerVariables.OPENCOUNT)) as CustomerVariables.OPENCOUNT,
        coalesce(newdf(CustomerVariables.FIRSTOPEN), full(CustomerVariables.FIRSTOPEN)) as CustomerVariables.FIRSTOPEN,
        coalesce(newdf(CustomerVariables.LASTOPEN), full(CustomerVariables.LASTOPEN)) as CustomerVariables.LASTOPEN,
        coalesce(newdf(CustomerVariables.MODEL), full(CustomerVariables.MODEL)) as CustomerVariables.MODEL,
        coalesce(newdf(CustomerVariables.VERSION), full(CustomerVariables.VERSION)) as CustomerVariables.VERSION,
        coalesce(newdf(CustomerVariables.LANGUAGE), full(CustomerVariables.LANGUAGE)) as CustomerVariables.LANGUAGE,
        coalesce(newdf(CustomerVariables.BUNDLEVERSION), full(CustomerVariables.BUNDLEVERSION)) as CustomerVariables.BUNDLEVERSION,
        coalesce(newdf(CustomerVariables.LAT), full(CustomerVariables.LAT)) as CustomerVariables.LAT,
        coalesce(newdf(CustomerVariables.LON), full(CustomerVariables.LON)) as CustomerVariables.LON,
        coalesce(newdf(CustomerVariables.ALTITUDE), full(CustomerVariables.ALTITUDE)) as CustomerVariables.ALTITUDE,
        coalesce(newdf(CustomerVariables.GEOLOCATION_CREATED), full(CustomerVariables.GEOLOCATION_CREATED)) as CustomerVariables.GEOLOCATION_CREATED,
        coalesce(newdf(CustomerVariables.VERSIONSDK), full(CustomerVariables.VERSIONSDK)) as CustomerVariables.VERSIONSDK,
        coalesce(newdf(CustomerVariables.FEEDBACK), full(CustomerVariables.FEEDBACK)) as CustomerVariables.FEEDBACK,
        coalesce(newdf(CustomerVariables.TIME_ZONE), full(CustomerVariables.TIME_ZONE)) as CustomerVariables.TIME_ZONE,
        coalesce(newdf(CustomerVariables.SYSTEM_OPTIN_NOTIFS), full(CustomerVariables.SYSTEM_OPTIN_NOTIFS)) as CustomerVariables.SYSTEM_OPTIN_NOTIFS,
        coalesce(newdf(CustomerVariables.ENABLED_NOTIFS), full(CustomerVariables.ENABLED_NOTIFS)) as CustomerVariables.ENABLED_NOTIFS,
        coalesce(newdf(CustomerVariables.ENABLED_INAPPS), full(CustomerVariables.ENABLED_INAPPS)) as CustomerVariables.ENABLED_INAPPS,
        coalesce(newdf(CustomerVariables.RANDOMID), full(CustomerVariables.RANDOMID)) as CustomerVariables.RANDOMID,
        coalesce(newdf(CustomerVariables.COUNTRYCODE), full(CustomerVariables.COUNTRYCODE)) as CustomerVariables.COUNTRYCODE,
        coalesce(newdf(CustomerVariables.AGGREGATED_NUMBER_OF_PURCHASES), full(CustomerVariables.AGGREGATED_NUMBER_OF_PURCHASES)) as CustomerVariables.AGGREGATED_NUMBER_OF_PURCHASES,
        coalesce(newdf(CustomerVariables.GENDER), full(CustomerVariables.GENDER)) as CustomerVariables.GENDER,
        coalesce(newdf(CustomerVariables.HAS_SHARED_PRODUCT), full(CustomerVariables.HAS_SHARED_PRODUCT)) as CustomerVariables.HAS_SHARED_PRODUCT,
        coalesce(newdf(CustomerVariables.LAST_ABANDONED_CART_DATE), full(CustomerVariables.LAST_ABANDONED_CART_DATE)) as CustomerVariables.LAST_ABANDONED_CART_DATE,
        coalesce(newdf(CustomerVariables.LAST_ABANDONED_CART_PRODUCT), full(CustomerVariables.LAST_ABANDONED_CART_PRODUCT)) as CustomerVariables.LAST_ABANDONED_CART_PRODUCT,
        coalesce(newdf(CustomerVariables.LASTORDERDATE), full(CustomerVariables.LASTORDERDATE)) as CustomerVariables.LASTORDERDATE,
        coalesce(newdf(CustomerVariables.LAST_SEARCH), full(CustomerVariables.LAST_SEARCH)) as CustomerVariables.LAST_SEARCH,
        coalesce(newdf(CustomerVariables.LAST_SEARCH_DATE), full(CustomerVariables.LAST_SEARCH_DATE)) as CustomerVariables.LAST_SEARCH_DATE,
        coalesce(newdf(CustomerVariables.LEAD), full(CustomerVariables.LEAD)) as CustomerVariables.LEAD,
        coalesce(newdf(CustomerVariables.LOGIN_USER_ID), full(CustomerVariables.LOGIN_USER_ID)) as CustomerVariables.LOGIN_USER_ID,
        coalesce(newdf(CustomerVariables.MOST_VISITED_CATEGORY), full(CustomerVariables.MOST_VISITED_CATEGORY)) as CustomerVariables.MOST_VISITED_CATEGORY,
        coalesce(newdf(CustomerVariables.ORDER_STATUS), full(CustomerVariables.ORDER_STATUS)) as CustomerVariables.ORDER_STATUS,
        coalesce(newdf(CustomerVariables.PURCHASE), full(CustomerVariables.PURCHASE)) as CustomerVariables.PURCHASE,
        coalesce(newdf(CustomerVariables.REGISTRATION), full(CustomerVariables.REGISTRATION)) as CustomerVariables.REGISTRATION,
        coalesce(newdf(CustomerVariables.STATUS_IN_APP), full(CustomerVariables.STATUS_IN_APP)) as CustomerVariables.STATUS_IN_APP,
        coalesce(newdf(CustomerVariables.WISHLIST_STATUS), full(CustomerVariables.WISHLIST_STATUS)) as CustomerVariables.WISHLIST_STATUS,
        coalesce(newdf(CustomerVariables.WISHLIST_ADD), full(CustomerVariables.WISHLIST_ADD)) as CustomerVariables.WISHLIST_ADD,
        coalesce(newdf(CustomerVariables.SHOP_COUNTRY), full(CustomerVariables.SHOP_COUNTRY)) as CustomerVariables.SHOP_COUNTRY,
        coalesce(newdf(CustomerVariables.AMOUNT_BASKET), full(CustomerVariables.AMOUNT_BASKET)) as CustomerVariables.AMOUNT_BASKET,
        coalesce(newdf(CustomerVariables.CART), full(CustomerVariables.CART)) as CustomerVariables.CART,
        coalesce(newdf(CustomerVariables.SPECIFIC_CATEGORY_VISIT_COUNT), full(CustomerVariables.SPECIFIC_CATEGORY_VISIT_COUNT)) as CustomerVariables.SPECIFIC_CATEGORY_VISIT_COUNT,
        coalesce(newdf(CustomerVariables.USER_NAME), full(CustomerVariables.USER_NAME)) as CustomerVariables.USER_NAME,
        coalesce(newdf(CustomerVariables.LAST_VIEWED_CATEGORY), full(CustomerVariables.LAST_VIEWED_CATEGORY)) as CustomerVariables.LAST_VIEWED_CATEGORY,
        coalesce(newdf(CustomerVariables.MAX_VISITED_CATEGORY), full(CustomerVariables.MAX_VISITED_CATEGORY)) as CustomerVariables.MAX_VISITED_CATEGORY,
        coalesce(newdf(CustomerVariables.MOST_VISITED_COUNTS), full(CustomerVariables.MOST_VISITED_COUNTS)) as CustomerVariables.MOST_VISITED_COUNTS,
        coalesce(newdf(CustomerVariables.SEARCH_DATE), full(CustomerVariables.SEARCH_DATE)) as CustomerVariables.SEARCH_DATE,
        coalesce(newdf(CustomerVariables.IDFA), full(CustomerVariables.IDFA)) as CustomerVariables.IDFA,
        coalesce(newdf(CustomerVariables.LAST_ORDER_DATE), full(CustomerVariables.LAST_ORDER_DATE)) as CustomerVariables.LAST_ORDER_DATE,
        coalesce(newdf(CustomerVariables.WISHLIST_PRODUCTS_COUNT), full(CustomerVariables.WISHLIST_PRODUCTS_COUNT)) as CustomerVariables.WISHLIST_PRODUCTS_COUNT,
        coalesce(newdf(CustomerVariables.RATED), full(CustomerVariables.RATED)) as CustomerVariables.RATED
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
