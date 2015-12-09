package com.jabong.dap.model.customer.campaigndata

import java.sql.{ Date, Timestamp }

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.CampaignMergedFields
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.dataFeeds.DataFeedsModel
import grizzled.slf4j.Logging
import org.apache.spark.sql.{ Row, DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.HashMap

/**
 * This File generates the conatct_list_mobile.csv for email campaigns.
 * Created by raghu on 17/8/15.
 *
 * UID - DCF
 * COUNTRY - "IN"
 * EMAIL - Customer
 * DOB - Customer
 * GENDER - Customer
 * REG_DATE - Customer
 * VERIFICATION_STATUS - Customer
 * AGE - Customer
 * PLATINUM_STATUS - Customer
 * FIRST_NAME - Customer, SalesAddress
 * LAST_NAME - Customer, SalesAddress
 * MOBILE - Custome, Sales Order
 * LAST_UPDATE_DATE - newsletter_subscription \ customer \ sales_order
 * EMAIL_SUBSCRIPTION_STATUS - newsletter_subscription
 * NL_SUB_DATE - newsletter_subscription
 * UNSUB_KEY - newsletter_subscription
 * MVP_TYPE - Customer Segement
 * SEGMENT - Customer_segment
 * DISCOUNT_SCORE - customer_segement
 * IS_REFERED - ???
 * NET_ORDERS - SalesOrderItem
 * FAV_BRAND - SalesOrderItem // not needed in csv file.
 * LAST_ORDER_DATE - SalesOrder
 * CITY - SalesAddress
 * CITY_TIER -
 * STATE_ZONE -
 * PHONE - duplicate
 * EMAIL_SUB_STATUS - duplicate
 * MOBILE_PERMISION_STATUS -
 * DND
 *
 */
object ContactListMobile extends DataFeedsModel with Logging {

  def canProcess(incrDate: String, saveMode: String): Boolean = {
    val pathContactListMobileFull = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CONTACT_LIST_MOBILE, DataSets.FULL_MERGE_MODE, incrDate)
    var res = DataWriter.canWrite(saveMode, pathContactListMobileFull)

    val pathContactListMobile = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CONTACT_LIST_MOBILE, DataSets.DAILY_MODE, incrDate)
    res = res || DataWriter.canWrite(saveMode, pathContactListMobile)

    res
  }

  def readDF(incrDate: String, prevDate: String, paths: String): HashMap[String, DataFrame] = {
    val dfMap = new HashMap[String, DataFrame]()
    var mode = DataSets.FULL_MERGE_MODE
    if (null == paths) {
      mode = DataSets.DAILY_MODE
      val contactListMobilePrevFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CONTACT_LIST_MOBILE, DataSets.FULL_MERGE_MODE, prevDate)
      dfMap.put("contactListMobilePrevFull", contactListMobilePrevFull)
    }

    val customerIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CUSTOMER, mode, incrDate)
    dfMap.put("customerIncr", customerIncr)

    val customerSegmentsIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CUSTOMER_SEGMENTS, mode, incrDate)
    dfMap.put("customerSegmentsIncr", customerSegmentsIncr)

    val nlsIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.NEWSLETTER_SUBSCRIPTION, mode, incrDate)
    dfMap.put("nlsIncr", nlsIncr)

    val customerOrdersIncr = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_ORDERS, mode, incrDate)
    dfMap.put("customerOrdersIncr", customerOrdersIncr)

    val dndFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, DataSets.DND, DataSets.FULL_MERGE_MODE, incrDate)
    dfMap.put("dndFull", dndFull)

    val smsOptOutFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, DataSets.SMS_OPT_OUT, DataSets.FULL_MERGE_MODE, incrDate)
    dfMap.put("smsOptOutFull", smsOptOutFull)

    val blockedNumbersFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.SOLUTIONS_INFINITI, DataSets.BLOCK_LIST_NUMBERS, DataSets.FULL_MERGE_MODE, incrDate)
    dfMap.put("blockedNumbersFull", blockedNumbersFull)

    val zoneCityFull = DataReader.getDataFrame4mCsv(ConfigConstants.ZONE_CITY_PINCODE_PATH, "true", ",")
    dfMap.put("zoneCityFull", zoneCityFull)

    val cmrFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, incrDate)
    dfMap.put("cmrFull", cmrFull)

    dfMap
  }

  /**
   * Process Method for the contact_list_mobile.csv generation for email campaigns.
   * @param dfMap Input parameters like dfs Read by readDF
   */
  def process(dfMap: HashMap[String, DataFrame]): HashMap[String, DataFrame] = {

    val contactListMobilePrevFull = dfMap.getOrElse("contactListMobilePrevFull", null)
    val customerSegmentsIncr = dfMap("customerSegmentsIncr")
    val customerIncr = dfMap("customerIncr")
    val nlsIncr = dfMap("nlsIncr")
    val dndFull = dfMap("dndFull")
    val smsOptOutFull = dfMap("smsOptOutFull")
    val blockedNumbersFull = dfMap("blockedNumbersFull")
    val zoneCityFull = dfMap("zoneCityFull")
    val cmrFull = dfMap("cmrFull")
    val customerOrdersIncr = dfMap("customerOrdersIncr")

    if (null == customerIncr || null == nlsIncr || null == customerSegmentsIncr || null == dndFull ||
      null == smsOptOutFull || null == blockedNumbersFull || null == zoneCityFull) {
      log("Data frame should not be null")
      return null
    }

    val smsOptOutMerged = smsOptOutFull.select(DNDVariables.MOBILE_NUMBER).unionAll(blockedNumbersFull.select(DNDVariables.MOBILE_NUMBER)).dropDuplicates()

    val dfMergedIncr = mergeIncrData(customerIncr, customerSegmentsIncr, nlsIncr, customerOrdersIncr, zoneCityFull, dndFull, smsOptOutMerged, cmrFull)

    val writeMap = new HashMap[String, DataFrame]()

    var contactListMobileIncr = dfMergedIncr
    var contactListMobileFull = dfMergedIncr

    if (null != contactListMobilePrevFull) {

      val dfIncrVarBC = Spark.getContext().broadcast(dfMergedIncr).value

      //join old and new data frame
      val joinDF = contactListMobilePrevFull.join(dfIncrVarBC, contactListMobilePrevFull(CustomerVariables.EMAIL) === dfIncrVarBC(CustomerVariables.EMAIL), SQL.FULL_OUTER)

      //merge old and new data frame
      val contactListMobileFull = joinDF.select(

        coalesce(dfIncrVarBC(ContactListMobileVars.UID), contactListMobilePrevFull(ContactListMobileVars.UID)) as ContactListMobileVars.UID,

        coalesce(dfIncrVarBC(CustomerVariables.EMAIL), contactListMobilePrevFull(CustomerVariables.EMAIL)) as CustomerVariables.EMAIL,

        coalesce(dfIncrVarBC(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS), contactListMobilePrevFull(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS)) as ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS,

        coalesce(dfIncrVarBC(CustomerVariables.PHONE), contactListMobilePrevFull(CustomerVariables.PHONE)) as CustomerVariables.PHONE,

        coalesce(dfIncrVarBC(ContactListMobileVars.MOBILE_PERMISION_STATUS), contactListMobilePrevFull(ContactListMobileVars.MOBILE_PERMISION_STATUS)) as ContactListMobileVars.MOBILE_PERMISION_STATUS,

        coalesce(dfIncrVarBC(ContactListMobileVars.CITY), contactListMobilePrevFull(ContactListMobileVars.CITY)) as ContactListMobileVars.CITY,

        coalesce(dfIncrVarBC(ContactListMobileVars.COUNTRY), contactListMobilePrevFull(ContactListMobileVars.COUNTRY)) as ContactListMobileVars.COUNTRY,

        coalesce(dfIncrVarBC(CustomerVariables.FIRST_NAME), contactListMobilePrevFull(CustomerVariables.FIRST_NAME)) as CustomerVariables.FIRST_NAME,

        coalesce(dfIncrVarBC(CustomerVariables.LAST_NAME), contactListMobilePrevFull(CustomerVariables.LAST_NAME)) as CustomerVariables.LAST_NAME,

        coalesce(dfIncrVarBC(ContactListMobileVars.DOB), contactListMobilePrevFull(ContactListMobileVars.DOB)) as ContactListMobileVars.DOB,

        coalesce(dfIncrVarBC(ContactListMobileVars.MVP_TYPE), contactListMobilePrevFull(ContactListMobileVars.MVP_TYPE)) as ContactListMobileVars.MVP_TYPE,

        dfIncrVarBC(ContactListMobileVars.NET_ORDERS).+(contactListMobilePrevFull(ContactListMobileVars.NET_ORDERS)) as ContactListMobileVars.NET_ORDERS,

        Udf.maxTimestamp(dfIncrVarBC(ContactListMobileVars.LAST_ORDER_DATE), contactListMobilePrevFull(ContactListMobileVars.LAST_ORDER_DATE)) as ContactListMobileVars.LAST_ORDER_DATE,

        coalesce(dfIncrVarBC(CustomerVariables.GENDER), contactListMobilePrevFull(CustomerVariables.GENDER)) as CustomerVariables.GENDER,

        Udf.minTimestamp(dfIncrVarBC(ContactListMobileVars.REG_DATE), contactListMobilePrevFull(ContactListMobileVars.REG_DATE)) as ContactListMobileVars.REG_DATE,

        coalesce(dfIncrVarBC(CustomerSegmentsVariables.SEGMENT), contactListMobilePrevFull(CustomerSegmentsVariables.SEGMENT)) as CustomerSegmentsVariables.SEGMENT,

        coalesce(dfIncrVarBC(ContactListMobileVars.AGE), contactListMobilePrevFull(ContactListMobileVars.AGE)) as ContactListMobileVars.AGE,

        coalesce(dfIncrVarBC(ContactListMobileVars.PLATINUM_STATUS), contactListMobilePrevFull(ContactListMobileVars.PLATINUM_STATUS)) as ContactListMobileVars.PLATINUM_STATUS,

        coalesce(dfIncrVarBC(ContactListMobileVars.IS_REFERED), contactListMobilePrevFull(ContactListMobileVars.IS_REFERED)) as ContactListMobileVars.IS_REFERED, //IS_REFERRED

        coalesce(dfIncrVarBC(ContactListMobileVars.NL_SUB_DATE), contactListMobilePrevFull(ContactListMobileVars.NL_SUB_DATE)) as ContactListMobileVars.NL_SUB_DATE,

        coalesce(dfIncrVarBC(ContactListMobileVars.VERIFICATION_STATUS), contactListMobilePrevFull(ContactListMobileVars.VERIFICATION_STATUS)) as ContactListMobileVars.VERIFICATION_STATUS,

        Udf.maxTimestamp(dfIncrVarBC(CustomerVariables.LAST_UPDATED_AT), contactListMobilePrevFull(CustomerVariables.LAST_UPDATED_AT)) as CustomerVariables.LAST_UPDATED_AT,

        coalesce(dfIncrVarBC(ContactListMobileVars.UNSUB_KEY), contactListMobilePrevFull(ContactListMobileVars.UNSUB_KEY)) as ContactListMobileVars.UNSUB_KEY,

        coalesce(dfIncrVarBC(ContactListMobileVars.CITY_TIER), contactListMobilePrevFull(ContactListMobileVars.CITY_TIER)) as ContactListMobileVars.CITY_TIER,

        coalesce(dfIncrVarBC(ContactListMobileVars.STATE_ZONE), contactListMobilePrevFull(ContactListMobileVars.STATE_ZONE)) as ContactListMobileVars.STATE_ZONE,

        coalesce(dfIncrVarBC(CustomerSegmentsVariables.DISCOUNT_SCORE), contactListMobilePrevFull(CustomerSegmentsVariables.DISCOUNT_SCORE)) as CustomerSegmentsVariables.DISCOUNT_SCORE,

        coalesce(dfIncrVarBC(CustomerVariables.ID_CUSTOMER), contactListMobilePrevFull(CustomerVariables.ID_CUSTOMER)) as CustomerVariables.ID_CUSTOMER,

        coalesce(dfIncrVarBC(CampaignMergedFields.DEVICE_ID), contactListMobilePrevFull(CampaignMergedFields.DEVICE_ID)) as CampaignMergedFields.DEVICE_ID,

        coalesce(dfIncrVarBC(SalesOrderItemVariables.FAV_BRAND), contactListMobilePrevFull(SalesOrderItemVariables.FAV_BRAND)) as SalesOrderItemVariables.FAV_BRAND,

        coalesce(dfIncrVarBC(NewsletterVariables.STATUS), contactListMobilePrevFull(NewsletterVariables.STATUS)) as NewsletterVariables.STATUS,

        coalesce(dfIncrVarBC(ContactListMobileVars.DND), contactListMobilePrevFull(ContactListMobileVars.DND)) as ContactListMobileVars.DND // DND
      )
      contactListMobileIncr = contactListMobileFull.except(contactListMobilePrevFull)
    }

    writeMap.put("contactListMobileFull", contactListMobileFull)

    val contactListMobileIncrCached = contactListMobileIncr.cache()
    writeMap.put("contactListMobileIncrCached", contactListMobileIncrCached)

    writeMap.put("contactListMobilePrevFull", contactListMobilePrevFull)

    writeMap
  }

  def write(dfWrite: HashMap[String, DataFrame], saveMode: String, incrDate: String) = {
    val pathContactListMobileFull = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CONTACT_LIST_MOBILE, DataSets.FULL_MERGE_MODE, incrDate)
    if (DataWriter.canWrite(saveMode, pathContactListMobileFull)) {
      DataWriter.writeParquet(dfWrite("contactListMobileFull"), pathContactListMobileFull, saveMode)
    }

    val contactListMobileIncrCached = dfWrite("contactListMobileIncrCached")
    val pathContactListMobile = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CONTACT_LIST_MOBILE, DataSets.DAILY_MODE, incrDate)
    if (DataWriter.canWrite(saveMode, pathContactListMobile)) {
      DataWriter.writeParquet(contactListMobileIncrCached, pathContactListMobile, saveMode)
    }

    val contactListMobilePrevFull = dfWrite.getOrElse("contactListMobilePrevFull", null)

    if (null != contactListMobilePrevFull) {
      val contactListMobilCsv = contactListMobileIncrCached.select(
        col(ContactListMobileVars.UID),
        Udf.maskForDecrypt(col(CustomerVariables.EMAIL), lit("**")) as ContactListMobileVars.EMAIL,
        col(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS),
        Udf.maskForDecrypt(col(CustomerVariables.PHONE), lit("##")) as ContactListMobileVars.MOBILE,
        col(ContactListMobileVars.MOBILE_PERMISION_STATUS),
        col(ContactListMobileVars.CITY) as ContactListMobileVars.CITY,
        col(ContactListMobileVars.COUNTRY),
        col(CustomerVariables.FIRST_NAME) as ContactListMobileVars.FIRST_NAME,
        col(CustomerVariables.LAST_NAME) as ContactListMobileVars.LAST_NAME,
        col(ContactListMobileVars.DOB),
        col(ContactListMobileVars.MVP_TYPE).cast(StringType) as ContactListMobileVars.MVP_TYPE,
        col(ContactListMobileVars.NET_ORDERS).cast(StringType) as ContactListMobileVars.NET_ORDERS,
        Udf.dateCsvFormat(col(ContactListMobileVars.LAST_ORDER_DATE)) as ContactListMobileVars.LAST_ORDER_DATE,
        col(CustomerVariables.GENDER) as ContactListMobileVars.GENDER,
        Udf.dateCsvFormat(col(ContactListMobileVars.REG_DATE)) as ContactListMobileVars.REG_DATE,
        col(CustomerSegmentsVariables.SEGMENT) as ContactListMobileVars.SEGMENT,
        col(ContactListMobileVars.AGE).cast(StringType) as ContactListMobileVars.AGE,
        col(ContactListMobileVars.PLATINUM_STATUS).cast(StringType) as ContactListMobileVars.PLATINUM_STATUS,
        col(ContactListMobileVars.IS_REFERED),
        Udf.dateCsvFormat(col(ContactListMobileVars.NL_SUB_DATE)) as ContactListMobileVars.NL_SUB_DATE,
        col(ContactListMobileVars.VERIFICATION_STATUS).cast(StringType) as ContactListMobileVars.VERIFICATION_STATUS,
        Udf.dateCsvFormat(col(CustomerVariables.LAST_UPDATED_AT)) as ContactListMobileVars.LAST_UPDATE_DATE,
        col(ContactListMobileVars.UNSUB_KEY),
        col(ContactListMobileVars.CITY_TIER),
        col(ContactListMobileVars.STATE_ZONE),
        col(CustomerSegmentsVariables.DISCOUNT_SCORE).cast(StringType) as ContactListMobileVars.DISCOUNT_SCORE
      ).na.fill("")

      var savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CONTACT_LIST_MOBILE, DataSets.DAILY_MODE, incrDate)
      DataWriter.writeParquet(contactListMobilCsv, savePath, saveMode)

      val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
      DataWriter.writeCsv(contactListMobilCsv, DataSets.VARIABLES, DataSets.CONTACT_LIST_MOBILE, DataSets.DAILY_MODE, incrDate, fileDate + "_CONTACTS_LIST", DataSets.IGNORE_SAVEMODE, "true", ";")

      val nlDataList = NewsletterDataList.getNLDataList(contactListMobileIncrCached, contactListMobilePrevFull)
      savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.NL_DATA_LIST, DataSets.DAILY_MODE, incrDate)
      DataWriter.writeParquet(nlDataList, savePath, saveMode)
      DataWriter.writeCsv(nlDataList, DataSets.VARIABLES, DataSets.NL_DATA_LIST, DataSets.DAILY_MODE, incrDate, fileDate + "_NL_data_list", DataSets.IGNORE_SAVEMODE, "true", ";")

      val appEmailFeed = AppEmailFeed.getAppEmailFeed(contactListMobileIncrCached, contactListMobilePrevFull)
      savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.APP_EMAIL_FEED, DataSets.DAILY_MODE, incrDate)
      DataWriter.writeParquet(appEmailFeed, savePath, saveMode)
      DataWriter.writeCsv(appEmailFeed, DataSets.VARIABLES, DataSets.APP_EMAIL_FEED, DataSets.DAILY_MODE, incrDate, fileDate + "_app_email_feed", DataSets.IGNORE_SAVEMODE, "true", ";")

      val contactListPlus = ContactListPlus.getContactListPlus(contactListMobileIncrCached, contactListMobilePrevFull)
      savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CONTACT_LIST_PLUS, DataSets.DAILY_MODE, incrDate)
      DataWriter.writeParquet(contactListPlus, savePath, saveMode)
      DataWriter.writeCsv(contactListPlus, DataSets.VARIABLES, DataSets.CONTACT_LIST_PLUS, DataSets.DAILY_MODE, incrDate, fileDate + "_Contact_list_Plus", DataSets.IGNORE_SAVEMODE, "true", ";")
    }
  }

  def getlatestNl(l: List[(Long, String, Timestamp, Timestamp, String)]): (Long, String, Timestamp, Timestamp, String) = {
    var t = Tuple5(l(0)._1, l(0)._2, l(0)._3, l(0)._4, l(0)._5)
    var maxts = TimeUtils.MIN_TIMESTAMP
    l.foreach{
      e =>
        if (e._4.after(maxts)) {
          t = Tuple5(e._1, e._2, e._3, e._4, e._5)
          maxts = e._4
        }
    }
    t
  }

  def getlatestCus(l: List[(Long, String, String, String, Date, String, Timestamp, String, Integer, Timestamp)]): (Long, String, String, String, Date, String, Timestamp, String, Integer, Timestamp) = {
    var t = Tuple10(l(0)._1, l(0)._2, l(0)._3, l(0)._4, l(0)._5, l(0)._6, l(0)._7, l(0)._8, l(0)._9, l(0)._10)
    var maxts = TimeUtils.MIN_TIMESTAMP
    l.foreach{
      e =>
        if (e._10.after(maxts)) {
          t = Tuple10(e._1, e._2, e._3, e._4, e._5, e._6, e._7, e._8, e._9, e._10)
          maxts = e._10
        }
    }
    t
  }

  def mergeIncrData(custIncr: DataFrame, custSegIncr: DataFrame, nlsIncr: DataFrame, customerOrdersIncr: DataFrame, cityZone: DataFrame, dnd: DataFrame, smsOptOut: DataFrame, cmrFull: DataFrame): DataFrame = {

    val schema1 = StructType(Array(
      StructField(NewsletterVariables.EMAIL, StringType, true),
      StructField(NewsletterVariables.FK_CUSTOMER, LongType, true),
      StructField(NewsletterVariables.STATUS, StringType, true),
      StructField(NewsletterVariables.CREATED_AT, TimestampType, true),
      StructField(NewsletterVariables.UPDATED_AT, TimestampType, true),
      StructField(NewsletterVariables.UNSUBSCRIBE_KEY, StringType, true)
    ))

    val schema2 = StructType(Array(
      StructField(CustomerVariables.EMAIL, StringType, true),
      StructField(CustomerVariables.ID_CUSTOMER, LongType, true),
      StructField(CustomerVariables.PHONE, StringType, true),
      StructField(CustomerVariables.FIRST_NAME, StringType, true),
      StructField(CustomerVariables.LAST_NAME, StringType, true),
      StructField(CustomerVariables.BIRTHDAY, DateType, true),
      StructField(CustomerVariables.GENDER, StringType, true),
      StructField(CustomerVariables.CREATED_AT, TimestampType, true),
      StructField(CustomerVariables.REWARD_TYPE, StringType, true),
      StructField(CustomerVariables.IS_CONFIRMED, IntegerType, true),
      StructField(CustomerVariables.UPDATED_AT, TimestampType, true)
    ))

    val nlsIncrMap = nlsIncr.select(NewsletterVariables.EMAIL,
      NewsletterVariables.FK_CUSTOMER,
      NewsletterVariables.STATUS,
      NewsletterVariables.CREATED_AT,
      NewsletterVariables.UPDATED_AT,
      NewsletterVariables.UNSUBSCRIBE_KEY)
      .map(e => (e(0).asInstanceOf[String] -> (e(1).asInstanceOf[Long], e(2).asInstanceOf[String], e(3).asInstanceOf[Timestamp], e(4).asInstanceOf[Timestamp], e(5).asInstanceOf[String]))).groupByKey()

    val n = nlsIncrMap.map(e => (e._1, getlatestNl(e._2.toList))).map(e => Row(e._1, e._2._1, e._2._2, e._2._3, e._2._4, e._2._5))

    val nlsIncrUnq = Spark.getSqlContext().createDataFrame(n, schema1)
    val custIncrMap = custIncr.select(CustomerVariables.EMAIL,
      CustomerVariables.ID_CUSTOMER,
      CustomerVariables.PHONE,
      CustomerVariables.FIRST_NAME,
      CustomerVariables.LAST_NAME,
      CustomerVariables.BIRTHDAY,
      CustomerVariables.GENDER,
      CustomerVariables.CREATED_AT,
      CustomerVariables.REWARD_TYPE,
      CustomerVariables.IS_CONFIRMED,
      CustomerVariables.UPDATED_AT)
      .map(e => (e(0).asInstanceOf[String] -> (e(1).asInstanceOf[Long], e(2).asInstanceOf[String],
      e(3).asInstanceOf[String], e(4).asInstanceOf[String], e(5).asInstanceOf[Date], e(6).asInstanceOf[String],
      e(7).asInstanceOf[Timestamp], e(8).asInstanceOf[String],
      e(9).asInstanceOf[Integer], e(10).asInstanceOf[Timestamp]))).groupByKey()
    val c = custIncrMap.map(e => (e._1, getlatestCus(e._2.toList))).map(e => Row(e._1, e._2._1, e._2._2, e._2._3, e._2._4, e._2._5, e._2._6, e._2._7, e._2._8, e._2._9, e._2._10))
    val custIncrUnq = Spark.getSqlContext().createDataFrame(c, schema2)

    val nlsJoined = custIncrUnq.join(nlsIncrUnq, custIncrUnq(CustomerVariables.EMAIL) === nlsIncrUnq(NewsletterVariables.EMAIL), SQL.FULL_OUTER)
      .select(
        custIncrUnq(CustomerVariables.ID_CUSTOMER),
        coalesce(custIncrUnq(CustomerVariables.EMAIL), nlsIncrUnq(NewsletterVariables.EMAIL)) as NewsletterVariables.EMAIL,
        Udf.udfEmailOptInStatus(nlsIncrUnq(NewsletterVariables.EMAIL), nlsIncrUnq(NewsletterVariables.STATUS))
          as ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS,
        custIncrUnq(CustomerVariables.PHONE),
        custIncrUnq(CustomerVariables.FIRST_NAME),
        custIncrUnq(CustomerVariables.LAST_NAME),
        custIncrUnq(CustomerVariables.BIRTHDAY) as ContactListMobileVars.DOB,
        custIncrUnq(CustomerVariables.GENDER),
        Udf.minTimestamp(custIncrUnq(CustomerVariables.CREATED_AT), nlsIncrUnq(NewsletterVariables.CREATED_AT))
          as ContactListMobileVars.REG_DATE,
        Udf.age(custIncrUnq(CustomerVariables.BIRTHDAY)) as ContactListMobileVars.AGE,
        Udf.platinumStatus(custIncrUnq(CustomerVariables.REWARD_TYPE)) as ContactListMobileVars.PLATINUM_STATUS,
        nlsIncrUnq(NewsletterVariables.CREATED_AT) as ContactListMobileVars.NL_SUB_DATE,
        custIncrUnq(CustomerVariables.IS_CONFIRMED) as ContactListMobileVars.VERIFICATION_STATUS,
        Udf.maxTimestamp(custIncrUnq(CustomerVariables.UPDATED_AT), nlsIncrUnq(NewsletterVariables.UPDATED_AT))
          as CustomerVariables.UPDATED_AT,
        nlsIncrUnq(NewsletterVariables.UNSUBSCRIBE_KEY) as ContactListMobileVars.UNSUB_KEY,
        nlsIncrUnq(NewsletterVariables.STATUS)
      )

    val custOrdersJoined = nlsJoined.join(customerOrdersIncr, nlsJoined(CustomerVariables.ID_CUSTOMER) === customerOrdersIncr(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
      .select(
        coalesce(nlsJoined(CustomerVariables.ID_CUSTOMER), customerOrdersIncr(SalesOrderVariables.FK_CUSTOMER)) as CustomerVariables.ID_CUSTOMER,
        nlsJoined(CustomerVariables.EMAIL),
        nlsJoined(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS),
        coalesce(nlsJoined(CustomerVariables.PHONE), customerOrdersIncr(CustomerVariables.PHONE)) as CustomerVariables.PHONE,
        customerOrdersIncr(ContactListMobileVars.CITY),
        customerOrdersIncr(ContactListMobileVars.CITY_TIER),
        customerOrdersIncr(ContactListMobileVars.STATE_ZONE),
        coalesce(nlsJoined(CustomerVariables.FIRST_NAME), customerOrdersIncr(CustomerVariables.FIRST_NAME)) as CustomerVariables.FIRST_NAME,
        coalesce(nlsJoined(CustomerVariables.LAST_NAME), customerOrdersIncr(CustomerVariables.LAST_NAME)) as CustomerVariables.LAST_NAME,
        nlsJoined(ContactListMobileVars.DOB),
        customerOrdersIncr(SalesOrderItemVariables.SUCCESSFUL_ORDERS) as ContactListMobileVars.NET_ORDERS,
        customerOrdersIncr(SalesOrderVariables.LAST_ORDER_DATE) as ContactListMobileVars.LAST_ORDER_DATE,
        nlsJoined(CustomerVariables.GENDER),
        nlsJoined(ContactListMobileVars.REG_DATE),
        nlsJoined(ContactListMobileVars.AGE),
        nlsJoined(ContactListMobileVars.PLATINUM_STATUS),
        nlsJoined(ContactListMobileVars.NL_SUB_DATE),
        nlsJoined(ContactListMobileVars.VERIFICATION_STATUS),
        Udf.maxTimestamp(customerOrdersIncr(SalesOrderVariables.LAST_ORDER_UPDATED_AT), nlsJoined(CustomerVariables.UPDATED_AT)) as CustomerVariables.LAST_UPDATED_AT,
        nlsJoined(ContactListMobileVars.UNSUB_KEY),
        nlsJoined(NewsletterVariables.STATUS),
        customerOrdersIncr(SalesOrderItemVariables.FAV_BRAND)
      )

    //  println("custOrdersJoined", custOrdersJoined.count())
    val custSegJoined = custOrdersJoined.join(custSegIncr, custOrdersJoined(CustomerVariables.ID_CUSTOMER) === custSegIncr(CustomerSegmentsVariables.FK_CUSTOMER), SQL.LEFT_OUTER)
      .select(
        coalesce(custOrdersJoined(CustomerVariables.ID_CUSTOMER), custSegIncr(CustomerSegmentsVariables.FK_CUSTOMER)) as CustomerVariables.ID_CUSTOMER,
        custOrdersJoined(CustomerVariables.EMAIL),
        custOrdersJoined(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS),
        custOrdersJoined(CustomerVariables.PHONE),
        custOrdersJoined(CustomerVariables.FIRST_NAME),
        custOrdersJoined(CustomerVariables.LAST_NAME),
        custOrdersJoined(ContactListMobileVars.DOB),
        custSegIncr(CustomerSegmentsVariables.MVP_SCORE) as ContactListMobileVars.MVP_TYPE,
        custOrdersJoined(CustomerVariables.GENDER),
        custOrdersJoined(ContactListMobileVars.REG_DATE),
        custSegIncr(CustomerSegmentsVariables.SEGMENT),
        custOrdersJoined(ContactListMobileVars.AGE),
        custOrdersJoined(ContactListMobileVars.PLATINUM_STATUS),
        custOrdersJoined(ContactListMobileVars.NL_SUB_DATE),
        custOrdersJoined(ContactListMobileVars.VERIFICATION_STATUS),
        custOrdersJoined(CustomerVariables.LAST_UPDATED_AT),
        custOrdersJoined(ContactListMobileVars.UNSUB_KEY),
        custSegIncr(CustomerSegmentsVariables.DISCOUNT_SCORE),
        custOrdersJoined(NewsletterVariables.STATUS),
        custOrdersJoined(SalesOrderItemVariables.FAV_BRAND),
        custOrdersJoined(ContactListMobileVars.CITY),
        custOrdersJoined(ContactListMobileVars.CITY_TIER),
        custOrdersJoined(ContactListMobileVars.STATE_ZONE),
        custOrdersJoined(ContactListMobileVars.NET_ORDERS),
        custOrdersJoined(ContactListMobileVars.LAST_ORDER_DATE)
      )
    //  println("custSegJoined " + custSegJoined.count())

    val dndBc = Spark.getContext().broadcast(dnd).value

    val dndMerged = custSegJoined.join(dndBc, custSegJoined(CustomerVariables.PHONE) === dndBc(DNDVariables.MOBILE_NUMBER), SQL.LEFT_OUTER)
      .select(
        custSegJoined(CustomerVariables.EMAIL),
        custSegJoined(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS),
        custSegJoined(CustomerVariables.PHONE),
        custSegJoined(ContactListMobileVars.CITY),
        custSegJoined(CustomerVariables.FIRST_NAME),
        custSegJoined(CustomerVariables.LAST_NAME),
        custSegJoined(ContactListMobileVars.DOB),
        custSegJoined(ContactListMobileVars.MVP_TYPE),
        custSegJoined(ContactListMobileVars.NET_ORDERS),
        custSegJoined(ContactListMobileVars.LAST_ORDER_DATE),
        custSegJoined(CustomerVariables.GENDER),
        custSegJoined(ContactListMobileVars.REG_DATE),
        custSegJoined(CustomerSegmentsVariables.SEGMENT),
        custSegJoined(ContactListMobileVars.AGE),
        custSegJoined(ContactListMobileVars.PLATINUM_STATUS),
        custSegJoined(ContactListMobileVars.NL_SUB_DATE),
        custSegJoined(ContactListMobileVars.VERIFICATION_STATUS),
        custSegJoined(CustomerVariables.LAST_UPDATED_AT),
        custSegJoined(ContactListMobileVars.UNSUB_KEY),
        custSegJoined(ContactListMobileVars.CITY_TIER),
        custSegJoined(ContactListMobileVars.STATE_ZONE),
        custSegJoined(CustomerSegmentsVariables.DISCOUNT_SCORE),
        custSegJoined(CustomerVariables.ID_CUSTOMER),
        custSegJoined(NewsletterVariables.STATUS),
        custSegJoined(SalesOrderItemVariables.FAV_BRAND),
        Udf.dnd(dndBc(DNDVariables.MOBILE_NUMBER)) as ContactListMobileVars.DND)

    val smsBc = Spark.getContext().broadcast(smsOptOut).value

    val smsOptJoined = dndMerged.join(smsBc, dndMerged(SalesAddressVariables.PHONE) === smsBc(DNDVariables.MOBILE_NUMBER), SQL.LEFT_OUTER)
      .select(
        dndMerged(CustomerVariables.EMAIL),
        dndMerged(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS),
        dndMerged(CustomerVariables.PHONE),
        Udf.mps(smsBc(DNDVariables.MOBILE_NUMBER)) as ContactListMobileVars.MOBILE_PERMISION_STATUS,
        dndMerged(ContactListMobileVars.CITY),
        dndMerged(CustomerVariables.FIRST_NAME),
        dndMerged(CustomerVariables.LAST_NAME),
        dndMerged(ContactListMobileVars.DOB),
        dndMerged(ContactListMobileVars.MVP_TYPE),
        dndMerged(ContactListMobileVars.NET_ORDERS),
        dndMerged(ContactListMobileVars.LAST_ORDER_DATE),
        dndMerged(CustomerVariables.GENDER),
        dndMerged(ContactListMobileVars.REG_DATE),
        dndMerged(CustomerSegmentsVariables.SEGMENT),
        dndMerged(ContactListMobileVars.AGE),
        dndMerged(ContactListMobileVars.PLATINUM_STATUS),
        dndMerged(ContactListMobileVars.NL_SUB_DATE),
        dndMerged(ContactListMobileVars.VERIFICATION_STATUS),
        dndMerged(CustomerVariables.LAST_UPDATED_AT),
        dndMerged(ContactListMobileVars.UNSUB_KEY),
        dndMerged(ContactListMobileVars.CITY_TIER),
        dndMerged(ContactListMobileVars.STATE_ZONE),
        dndMerged(CustomerSegmentsVariables.DISCOUNT_SCORE),
        dndMerged(CustomerVariables.ID_CUSTOMER),
        dndMerged(NewsletterVariables.STATUS),
        dndMerged(SalesOrderItemVariables.FAV_BRAND),
        dndMerged(ContactListMobileVars.DND)
      )
    val cmrFullFil = cmrFull.filter(cmrFull(CustomerVariables.EMAIL).isNotNull)
    val res = smsOptJoined.join(cmrFullFil, cmrFullFil(CustomerVariables.EMAIL) === smsOptJoined(CustomerVariables.EMAIL), SQL.LEFT_OUTER)
      .select(
        cmrFullFil(ContactListMobileVars.UID),
        smsOptJoined(CustomerVariables.EMAIL),
        smsOptJoined(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS),
        smsOptJoined(CustomerVariables.PHONE),
        smsOptJoined(ContactListMobileVars.MOBILE_PERMISION_STATUS),
        smsOptJoined(ContactListMobileVars.CITY),
        lit("IN") as ContactListMobileVars.COUNTRY,
        smsOptJoined(CustomerVariables.FIRST_NAME),
        smsOptJoined(CustomerVariables.LAST_NAME),
        smsOptJoined(ContactListMobileVars.DOB),
        smsOptJoined(ContactListMobileVars.MVP_TYPE),
        smsOptJoined(ContactListMobileVars.NET_ORDERS),
        smsOptJoined(ContactListMobileVars.LAST_ORDER_DATE),
        smsOptJoined(CustomerVariables.GENDER),
        smsOptJoined(ContactListMobileVars.REG_DATE),
        smsOptJoined(CustomerSegmentsVariables.SEGMENT),
        smsOptJoined(ContactListMobileVars.AGE),
        smsOptJoined(ContactListMobileVars.PLATINUM_STATUS),
        lit("") as ContactListMobileVars.IS_REFERED,
        smsOptJoined(ContactListMobileVars.NL_SUB_DATE),
        smsOptJoined(ContactListMobileVars.VERIFICATION_STATUS),
        smsOptJoined(CustomerVariables.LAST_UPDATED_AT),
        smsOptJoined(ContactListMobileVars.UNSUB_KEY),
        smsOptJoined(ContactListMobileVars.CITY_TIER),
        smsOptJoined(ContactListMobileVars.STATE_ZONE),
        smsOptJoined(CustomerSegmentsVariables.DISCOUNT_SCORE),
        smsOptJoined(CustomerVariables.ID_CUSTOMER),
        smsOptJoined(NewsletterVariables.STATUS),
        smsOptJoined(SalesOrderItemVariables.FAV_BRAND),
        smsOptJoined(ContactListMobileVars.DND),
        Udf.device(cmrFullFil(PageVisitVariables.DOMAIN), cmrFullFil(PageVisitVariables.BROWSER_ID), lit(null)) as CampaignMergedFields.DEVICE_ID
      )
    res
  }
}