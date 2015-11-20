package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.CampaignMergedFields
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ ContactListMobileVars, _ }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.customer.variables.CustomerSegments
import com.jabong.dap.model.dataFeeds.DataFeedsModel
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

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

    val dndFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.RESPONSYS, DataSets.DND, DataSets.FULL_MERGE_MODE, incrDate)
    dfMap.put("dndFull", dndFull)

    val smsOptOutFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.RESPONSYS, DataSets.SMS_OPT_OUT, DataSets.FULL_MERGE_MODE, incrDate)
    dfMap.put("smsOptOutFull", smsOptOutFull)

    val blockedNumbersFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.SOLUTIONS_INFINITI, DataSets.BLOCK_LIST_NUMBERS, DataSets.FULL_MERGE_MODE, incrDate)
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

    val dfCustSegCalcIncr = CustomerSegments.getCustomerSegments(customerSegmentsIncr)
    //FK_CUSTOMER, MVP_TYPE, SEGMENT, DISCOUNT_SCORE

    val dfSmsOptOutMerged = smsOptOutFull.select(DNDVariables.MOBILE_NUMBER).unionAll(blockedNumbersFull.select(DNDVariables.MOBILE_NUMBER)).dropDuplicates()

    val dfMergedIncr = mergeIncrData(customerIncr, dfCustSegCalcIncr, nlsIncr, customerOrdersIncr,
      zoneCityFull, dndFull, dfSmsOptOutMerged, cmrFull)

    val writeMap = new HashMap[String, DataFrame]()

    var contactListMobileIncr = dfMergedIncr
    var contactListMobileFull = dfMergedIncr

    if (null != dfContactListMobilePrevFull) {

      val dfIncrVarBC = Spark.getContext().broadcast(dfMergedIncr).value

      //join old and new data frame
      val joinDF = dfContactListMobilePrevFull.join(dfIncrVarBC, dfContactListMobilePrevFull(CustomerVariables.ID_CUSTOMER) === dfIncrVarBC(CustomerVariables.ID_CUSTOMER), SQL.FULL_OUTER)

      //merge old and new data frame
      val contactListMobileFull = joinDF.select(

        coalesce(dfIncrVarBC(ContactListMobileVars.UID), dfContactListMobilePrevFull(ContactListMobileVars.UID)) as ContactListMobileVars.UID,

        coalesce(dfIncrVarBC(CustomerVariables.EMAIL), dfContactListMobilePrevFull(CustomerVariables.EMAIL)) as CustomerVariables.EMAIL,

        coalesce(dfIncrVarBC(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS), dfContactListMobilePrevFull(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS)) as ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS,

        coalesce(dfIncrVarBC(CustomerVariables.PHONE), dfContactListMobilePrevFull(CustomerVariables.PHONE)) as CustomerVariables.PHONE,

        coalesce(dfIncrVarBC(ContactListMobileVars.MOBILE_PERMISION_STATUS), dfContactListMobilePrevFull(ContactListMobileVars.MOBILE_PERMISION_STATUS)) as ContactListMobileVars.MOBILE_PERMISION_STATUS,

        coalesce(dfIncrVarBC(CustomerVariables.CITY), dfContactListMobilePrevFull(CustomerVariables.CITY)) as CustomerVariables.CITY,

        coalesce(dfIncrVarBC(ContactListMobileVars.COUNTRY), dfContactListMobilePrevFull(ContactListMobileVars.COUNTRY)) as ContactListMobileVars.COUNTRY,

        coalesce(dfIncrVarBC(CustomerVariables.FIRST_NAME), dfContactListMobilePrevFull(CustomerVariables.FIRST_NAME)) as CustomerVariables.FIRST_NAME,

        coalesce(dfIncrVarBC(CustomerVariables.LAST_NAME), dfContactListMobilePrevFull(CustomerVariables.LAST_NAME)) as CustomerVariables.LAST_NAME,

        coalesce(dfIncrVarBC(ContactListMobileVars.DOB), dfContactListMobilePrevFull(ContactListMobileVars.DOB)) as ContactListMobileVars.DOB,

        coalesce(dfIncrVarBC(ContactListMobileVars.MVP_TYPE), dfContactListMobilePrevFull(ContactListMobileVars.MVP_TYPE)) as ContactListMobileVars.MVP_TYPE,

        dfIncrVarBC(ContactListMobileVars.NET_ORDERS).+(dfContactListMobilePrevFull(ContactListMobileVars.NET_ORDERS)) as ContactListMobileVars.NET_ORDERS,

        Udf.maxTimestamp(dfIncrVarBC(ContactListMobileVars.LAST_ORDER_DATE), dfContactListMobilePrevFull(ContactListMobileVars.LAST_ORDER_DATE)) as ContactListMobileVars.LAST_ORDER_DATE,

        coalesce(dfIncrVarBC(CustomerVariables.GENDER), dfContactListMobilePrevFull(CustomerVariables.GENDER)) as CustomerVariables.GENDER,

        Udf.minTimestamp(dfIncrVarBC(ContactListMobileVars.REG_DATE), dfContactListMobilePrevFull(ContactListMobileVars.REG_DATE)) as ContactListMobileVars.REG_DATE,

        coalesce(dfIncrVarBC(CustomerSegmentsVariables.SEGMENT), dfContactListMobilePrevFull(CustomerSegmentsVariables.SEGMENT)) as CustomerSegmentsVariables.SEGMENT,

        coalesce(dfIncrVarBC(ContactListMobileVars.AGE), dfContactListMobilePrevFull(ContactListMobileVars.AGE)) as ContactListMobileVars.AGE,

        coalesce(dfIncrVarBC(ContactListMobileVars.PLATINUM_STATUS), dfContactListMobilePrevFull(ContactListMobileVars.PLATINUM_STATUS)) as ContactListMobileVars.PLATINUM_STATUS,

        coalesce(dfIncrVarBC(ContactListMobileVars.IS_REFERED), dfContactListMobilePrevFull(ContactListMobileVars.IS_REFERED)) as ContactListMobileVars.IS_REFERED, //IS_REFERRED

        coalesce(dfIncrVarBC(ContactListMobileVars.NL_SUB_DATE), dfContactListMobilePrevFull(ContactListMobileVars.NL_SUB_DATE)) as ContactListMobileVars.NL_SUB_DATE,

        coalesce(dfIncrVarBC(ContactListMobileVars.VERIFICATION_STATUS), dfContactListMobilePrevFull(ContactListMobileVars.VERIFICATION_STATUS)) as ContactListMobileVars.VERIFICATION_STATUS,

        Udf.maxTimestamp(dfIncrVarBC(CustomerVariables.LAST_UPDATED_AT), dfContactListMobilePrevFull(CustomerVariables.LAST_UPDATED_AT)) as CustomerVariables.LAST_UPDATED_AT,

        coalesce(dfIncrVarBC(ContactListMobileVars.UNSUB_KEY), dfContactListMobilePrevFull(ContactListMobileVars.UNSUB_KEY)) as ContactListMobileVars.UNSUB_KEY,

        coalesce(dfIncrVarBC(ContactListMobileVars.CITY_TIER), dfContactListMobilePrevFull(ContactListMobileVars.CITY_TIER)) as ContactListMobileVars.CITY_TIER,

        coalesce(dfIncrVarBC(ContactListMobileVars.STATE_ZONE), dfContactListMobilePrevFull(ContactListMobileVars.STATE_ZONE)) as ContactListMobileVars.STATE_ZONE,

        coalesce(dfIncrVarBC(CustomerSegmentsVariables.DISCOUNT_SCORE), dfContactListMobilePrevFull(CustomerSegmentsVariables.DISCOUNT_SCORE)) as CustomerSegmentsVariables.DISCOUNT_SCORE,

        coalesce(dfIncrVarBC(CustomerVariables.ID_CUSTOMER), dfContactListMobilePrevFull(CustomerVariables.ID_CUSTOMER)) as CustomerVariables.ID_CUSTOMER,

        coalesce(dfIncrVarBC(CampaignMergedFields.DEVICE_ID), dfContactListMobilePrevFull(CampaignMergedFields.DEVICE_ID)) as CampaignMergedFields.DEVICE_ID,

        coalesce(dfIncrVarBC(SalesOrderItemVariables.FAV_BRAND), dfContactListMobilePrevFull(SalesOrderItemVariables.FAV_BRAND)) as SalesOrderItemVariables.FAV_BRAND,

        coalesce(dfIncrVarBC(NewsletterVariables.STATUS), dfContactListMobilePrevFull(NewsletterVariables.STATUS)) as NewsletterVariables.STATUS,

        coalesce(dfIncrVarBC(ContactListMobileVars.DND), dfContactListMobilePrevFull(ContactListMobileVars.DND)) as ContactListMobileVars.DND // DND
      )
      contactListMobileIncr = contactListMobileFull.except(dfContactListMobilePrevFull)
    }

    writeMap.put("contactListMobileFull", contactListMobileFull)

    val dfContactListMobileIncrCached = contactListMobileIncr.cache()
    writeMap.put("contactListMobileIncrCached", dfContactListMobileIncrCached)

    writeMap.put("contactListMobilePrevFull", dfContactListMobilePrevFull)

    writeMap
  }

  def write(dfWrite: HashMap[String, DataFrame], saveMode: String, incrDate: String) = {
    val pathContactListMobileFull = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CONTACT_LIST_MOBILE, DataSets.FULL_MERGE_MODE, incrDate)
    if (DataWriter.canWrite(saveMode, pathContactListMobileFull)) {
      DataWriter.writeParquet(dfWrite("contactListMobileFull"), pathContactListMobileFull, saveMode)
    }

    val dfContactListMobileIncrCached = dfWrite("contactListMobileIncrCached")
    val pathContactListMobile = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CONTACT_LIST_MOBILE, DataSets.DAILY_MODE, incrDate)
    if (DataWriter.canWrite(saveMode, pathContactListMobile)) {
      DataWriter.writeParquet(dfContactListMobileIncrCached, pathContactListMobile, saveMode)
    }

    val dfContactListMobilePrevFull = dfWrite.getOrElse("contactListMobilePrevFull", null)

    if (null != dfContactListMobilePrevFull) {
      val dfCsv = dfContactListMobileIncrCached.select(
        col(ContactListMobileVars.UID),
        col(CustomerVariables.EMAIL) as ContactListMobileVars.EMAIL,
        col(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS),
        col(CustomerVariables.PHONE) as ContactListMobileVars.MOBILE,
        col(ContactListMobileVars.MOBILE_PERMISION_STATUS),
        col(CustomerVariables.CITY) as ContactListMobileVars.CITY,
        col(ContactListMobileVars.COUNTRY),
        col(CustomerVariables.FIRST_NAME) as ContactListMobileVars.FIRST_NAME,
        col(CustomerVariables.LAST_NAME) as ContactListMobileVars.LAST_NAME,
        col(ContactListMobileVars.DOB),
        col(ContactListMobileVars.MVP_TYPE),
        col(ContactListMobileVars.NET_ORDERS),
        Udf.dateCsvFormat(col(ContactListMobileVars.LAST_ORDER_DATE)) as ContactListMobileVars.LAST_ORDER_DATE,
        col(CustomerVariables.GENDER) as ContactListMobileVars.GENDER,
        Udf.dateCsvFormat(col(ContactListMobileVars.REG_DATE)) as ContactListMobileVars.REG_DATE,
        col(CustomerSegmentsVariables.SEGMENT) as ContactListMobileVars.SEGMENT,
        col(ContactListMobileVars.AGE),
        col(ContactListMobileVars.PLATINUM_STATUS),
        col(ContactListMobileVars.IS_REFERED),
        Udf.dateCsvFormat(col(ContactListMobileVars.NL_SUB_DATE)) as ContactListMobileVars.NL_SUB_DATE,
        col(ContactListMobileVars.VERIFICATION_STATUS),
        Udf.dateCsvFormat(col(CustomerVariables.LAST_UPDATED_AT)) as ContactListMobileVars.LAST_UPDATE_DATE,
        col(ContactListMobileVars.UNSUB_KEY),
        col(ContactListMobileVars.CITY_TIER),
        col(ContactListMobileVars.STATE_ZONE),
        col(CustomerSegmentsVariables.DISCOUNT_SCORE) as ContactListMobileVars.DISCOUNT_SCORE
      ).na.fill("")

      val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
      DataWriter.writeCsv(dfCsv, DataSets.VARIABLES, DataSets.CONTACT_LIST_MOBILE, DataSets.DAILY_MODE, incrDate, fileDate + "_CONTACTS_LIST", DataSets.IGNORE_SAVEMODE, "true", ";")

      val dfNlDataList = NewsletterDataList.getNLDataList(dfContactListMobileIncrCached, dfContactListMobilePrevFull)
      DataWriter.writeCsv(dfNlDataList, DataSets.VARIABLES, DataSets.NL_DATA_LIST, DataSets.DAILY_MODE, incrDate, fileDate + "_NL_data_list", DataSets.IGNORE_SAVEMODE, "true", ";")

      val dfAppEmailFeed = AppEmailFeed.getAppEmailFeed(dfContactListMobileIncrCached, dfContactListMobilePrevFull)
      DataWriter.writeCsv(dfAppEmailFeed, DataSets.VARIABLES, DataSets.APP_EMAIL_FEED, DataSets.DAILY_MODE, incrDate, fileDate + "_app_email_feed", DataSets.IGNORE_SAVEMODE, "true", ";")

      val dfContactListPlus = ContactListPlus.getContactListPlus(dfContactListMobileIncrCached, dfContactListMobilePrevFull)
      DataWriter.writeCsv(dfContactListPlus, DataSets.VARIABLES, DataSets.CONTACT_LIST_PLUS, DataSets.DAILY_MODE, incrDate, fileDate + "_Contact_list_Plus", DataSets.IGNORE_SAVEMODE, "true", ";")
    }
  }

  def mergeIncrData(customerIncr: DataFrame, custSegCalcIncr: DataFrame, nls: DataFrame, customerOrdersIncr: DataFrame, cityZone: DataFrame, dnd: DataFrame, smsOptOut: DataFrame, dfCmrFull: DataFrame): DataFrame = {
    val customerNls = customerIncr.join(nls, customerIncr(CustomerVariables.EMAIL) === nls(NewsletterVariables.EMAIL), SQL.FULL_OUTER)
      .select(
        coalesce(customerIncr(CustomerVariables.ID_CUSTOMER), nls(NewsletterVariables.FK_CUSTOMER)) as CustomerVariables.ID_CUSTOMER,
        customerIncr(CustomerVariables.EMAIL),
        Udf.udfEmailOptInStatus(nls(NewsletterVariables.EMAIL), nls(NewsletterVariables.STATUS)) as ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS,
        customerIncr(CustomerVariables.PHONE),
        customerIncr(CustomerVariables.FIRST_NAME),
        customerIncr(CustomerVariables.LAST_NAME),
        customerIncr(CustomerVariables.BIRTHDAY) as ContactListMobileVars.DOB,
        customerIncr(CustomerVariables.GENDER),
        Udf.minTimestamp(customerIncr(CustomerVariables.CREATED_AT), nls(NewsletterVariables.CREATED_AT)) as ContactListMobileVars.REG_DATE,
        Udf.age(customerIncr(CustomerVariables.BIRTHDAY)) as ContactListMobileVars.AGE,
        Udf.platinumStatus(customerIncr(CustomerVariables.REWARD_TYPE)) as ContactListMobileVars.PLATINUM_STATUS,
        nls(NewsletterVariables.CREATED_AT) as ContactListMobileVars.NL_SUB_DATE,
        customerIncr(CustomerVariables.IS_CONFIRMED) as ContactListMobileVars.VERIFICATION_STATUS,
        Udf.maxTimestamp(customerIncr(CustomerVariables.UPDATED_AT), nls(NewsletterVariables.UPDATED_AT)) as CustomerVariables.UPDATED_AT,
        nls(NewsletterVariables.UNSUBSCRIBE_KEY) as ContactListMobileVars.UNSUB_KEY,
        nls(NewsletterVariables.STATUS)
      )

    val customerMerged = customerNls.join(custSegCalcIncr, customerNls(CustomerVariables.ID_CUSTOMER) === custSegCalcIncr(CustomerSegmentsVariables.FK_CUSTOMER), SQL.FULL_OUTER)
      .select(
        coalesce(customerNls(CustomerVariables.ID_CUSTOMER), custSegCalcIncr(CustomerSegmentsVariables.FK_CUSTOMER)) as CustomerVariables.ID_CUSTOMER,
        customerNls(CustomerVariables.EMAIL),
        customerNls(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS),
        customerNls(CustomerVariables.PHONE),
        customerNls(CustomerVariables.FIRST_NAME),
        customerNls(CustomerVariables.LAST_NAME),
        customerNls(ContactListMobileVars.DOB),
        custSegCalcIncr(ContactListMobileVars.MVP_TYPE),
        customerNls(CustomerVariables.GENDER),
        customerNls(ContactListMobileVars.REG_DATE),
        custSegCalcIncr(CustomerSegmentsVariables.SEGMENT),
        customerNls(ContactListMobileVars.AGE),
        customerNls(ContactListMobileVars.PLATINUM_STATUS),
        customerNls(ContactListMobileVars.NL_SUB_DATE),
        customerNls(ContactListMobileVars.VERIFICATION_STATUS),
        customerNls(CustomerVariables.UPDATED_AT),
        customerNls(ContactListMobileVars.UNSUB_KEY),
        custSegCalcIncr(CustomerSegmentsVariables.DISCOUNT_SCORE),
        customerNls(NewsletterVariables.STATUS)
      )

    val mergedIncr = customerMerged.join(customerOrdersIncr, customerMerged(CustomerVariables.ID_CUSTOMER) === customerOrdersIncr(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
      .select(
        customerMerged(CustomerVariables.EMAIL),
        customerMerged(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS),
        coalesce(customerMerged(CustomerVariables.PHONE), customerOrdersIncr(CustomerVariables.PHONE)) as CustomerVariables.PHONE,
        customerOrdersIncr(ContactListMobileVars.CITY),
        customerOrdersIncr(ContactListMobileVars.CITY_TIER),
        customerOrdersIncr(ContactListMobileVars.STATE_ZONE),
        coalesce(customerMerged(CustomerVariables.FIRST_NAME), customerOrdersIncr(CustomerVariables.FIRST_NAME)) as CustomerVariables.FIRST_NAME,
        coalesce(customerMerged(CustomerVariables.LAST_NAME), customerOrdersIncr(CustomerVariables.LAST_NAME)) as CustomerVariables.LAST_NAME,
        customerMerged(ContactListMobileVars.DOB),
        customerMerged(ContactListMobileVars.MVP_TYPE),
        customerOrdersIncr(SalesOrderItemVariables.SUCCESSFUL_ORDERS) as ContactListMobileVars.NET_ORDERS,
        customerOrdersIncr(SalesOrderVariables.LAST_ORDER_DATE) as ContactListMobileVars.LAST_ORDER_DATE,
        customerMerged(CustomerVariables.GENDER),
        customerMerged(ContactListMobileVars.REG_DATE),
        customerMerged(CustomerSegmentsVariables.SEGMENT),
        customerMerged(ContactListMobileVars.AGE),
        customerMerged(ContactListMobileVars.PLATINUM_STATUS),
        customerMerged(ContactListMobileVars.NL_SUB_DATE),
        customerMerged(ContactListMobileVars.VERIFICATION_STATUS),
        Udf.maxTimestamp(customerOrdersIncr(SalesOrderVariables.LAST_ORDER_UPDATED_AT), customerMerged(CustomerVariables.UPDATED_AT)) as CustomerVariables.LAST_UPDATED_AT,
        customerMerged(ContactListMobileVars.UNSUB_KEY),
        customerMerged(CustomerSegmentsVariables.DISCOUNT_SCORE),
        coalesce(customerMerged(CustomerVariables.ID_CUSTOMER), customerOrdersIncr(SalesOrderVariables.FK_CUSTOMER)) as CustomerVariables.ID_CUSTOMER,
        customerMerged(NewsletterVariables.STATUS),
        customerOrdersIncr(SalesOrderItemVariables.FAV_BRAND)
      )

    val dndBc = Spark.getContext().broadcast(dnd).value

    val dndMerged = mergedIncr.join(dndBc, mergedIncr(CustomerVariables.PHONE) === dndBc(DNDVariables.MOBILE_NUMBER), SQL.LEFT_OUTER)
      .select(
        mergedIncr(CustomerVariables.EMAIL),
        mergedIncr(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS),
        mergedIncr(CustomerVariables.PHONE),
        mergedIncr(ContactListMobileVars.CITY),
        mergedIncr(CustomerVariables.FIRST_NAME),
        mergedIncr(CustomerVariables.LAST_NAME),
        mergedIncr(ContactListMobileVars.DOB),
        mergedIncr(ContactListMobileVars.MVP_TYPE),
        mergedIncr(ContactListMobileVars.NET_ORDERS),
        mergedIncr(ContactListMobileVars.LAST_ORDER_DATE),
        mergedIncr(CustomerVariables.GENDER),
        mergedIncr(ContactListMobileVars.REG_DATE),
        mergedIncr(CustomerSegmentsVariables.SEGMENT),
        mergedIncr(ContactListMobileVars.AGE),
        mergedIncr(ContactListMobileVars.PLATINUM_STATUS),
        mergedIncr(ContactListMobileVars.NL_SUB_DATE),
        mergedIncr(ContactListMobileVars.VERIFICATION_STATUS),
        mergedIncr(CustomerVariables.LAST_UPDATED_AT),
        mergedIncr(ContactListMobileVars.UNSUB_KEY),
        mergedIncr(ContactListMobileVars.CITY_TIER),
        mergedIncr(ContactListMobileVars.STATE_ZONE),
        mergedIncr(CustomerSegmentsVariables.DISCOUNT_SCORE),
        mergedIncr(CustomerVariables.ID_CUSTOMER),
        mergedIncr(NewsletterVariables.STATUS),
        mergedIncr(SalesOrderItemVariables.FAV_BRAND),
        Udf.dnd(dndBc(DNDVariables.MOBILE_NUMBER)) as ContactListMobileVars.DND)

    val smsBc = Spark.getContext().broadcast(smsOptOut).value

    val dfJoined = dndMerged.join(smsBc, dndMerged(SalesAddressVariables.PHONE) === smsBc(DNDVariables.MOBILE_NUMBER), SQL.LEFT_OUTER)
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

    val res = dfJoined.join(dfCmrFull, dfCmrFull(CustomerVariables.EMAIL) === dfJoined(CustomerVariables.EMAIL), SQL.LEFT_OUTER)
      .select(
        dfCmrFull(ContactListMobileVars.UID),
        dfJoined(CustomerVariables.EMAIL),
        dfJoined(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS),
        dfJoined(CustomerVariables.PHONE),
        dfJoined(ContactListMobileVars.MOBILE_PERMISION_STATUS),
        dfJoined(ContactListMobileVars.CITY),
        lit("IN") as ContactListMobileVars.COUNTRY,
        dfJoined(CustomerVariables.FIRST_NAME),
        dfJoined(CustomerVariables.LAST_NAME),
        dfJoined(ContactListMobileVars.DOB),
        dfJoined(ContactListMobileVars.MVP_TYPE),
        dfJoined(ContactListMobileVars.NET_ORDERS),
        dfJoined(ContactListMobileVars.LAST_ORDER_DATE),
        dfJoined(CustomerVariables.GENDER),
        dfJoined(ContactListMobileVars.REG_DATE),
        dfJoined(CustomerSegmentsVariables.SEGMENT),
        dfJoined(ContactListMobileVars.AGE),
        dfJoined(ContactListMobileVars.PLATINUM_STATUS),
        lit("") as ContactListMobileVars.IS_REFERED,
        dfJoined(ContactListMobileVars.NL_SUB_DATE),
        dfJoined(ContactListMobileVars.VERIFICATION_STATUS),
        dfJoined(CustomerVariables.LAST_UPDATED_AT),
        dfJoined(ContactListMobileVars.UNSUB_KEY),
        dfJoined(ContactListMobileVars.CITY_TIER),
        dfJoined(ContactListMobileVars.STATE_ZONE),
        dfJoined(CustomerSegmentsVariables.DISCOUNT_SCORE),
        dfJoined(CustomerVariables.ID_CUSTOMER),
        dfJoined(NewsletterVariables.STATUS),
        dfJoined(SalesOrderItemVariables.FAV_BRAND),
        dfJoined(ContactListMobileVars.DND),
        Udf.device(dfCmrFull(PageVisitVariables.DOMAIN), dfCmrFull(PageVisitVariables.BROWSER_ID), lit(null)) as CampaignMergedFields.DEVICE_ID
      )
    res
  }
}