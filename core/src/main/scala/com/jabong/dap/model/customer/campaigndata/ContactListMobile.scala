package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.common.schema.SchemaUtils
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.common.{ OptionUtils, Spark }
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.MergeUtils
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.customer.variables.{ Customer, CustomerSegments }
import com.jabong.dap.model.order.variables.{ SalesOrder, SalesOrderAddress, SalesOrderItem }
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

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
object ContactListMobile extends Logging {

  val udfEmailOptInStatus = udf((nls_email: String, status: String) => Customer.getEmailOptInStatus(nls_email: String, status: String))

  /**
   * Start Method for the contact_list_mobile.csv generation for email campaigns.
   * @param params Input parameters like for which date to do and saveMode, Etc.
   */
  def start(params: ParamInfo) = {

    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val saveMode = params.saveMode
    val paths = OptionUtils.getOptValue(params.path)
    val prevDate = OptionUtils.getOptValue(params.fullDate, TimeUtils.getDateAfterNDays(-2, TimeConstants.DATE_FORMAT_FOLDER))

    //read Data Frames
    val (
      dfCustomerIncr,
      dfCustomerListMobilePrevFull,
      dfCustomerSegmentsIncr,
      dfNLSIncr,
      dfSalesOrderIncr,
      dfSalesOrderFull,
      dfSalesOrderAddrFull,
      dfSalesOrderAddrFavPrevFull,
      dfSalesOrderItemIncr,
      dfSalesOrderCalcPrevFull,
      dfSalesOrderItemCalcPrevFull,
      dfDND,
      dfSmsOptOut,
      dfBlockedNumbers,
      dfZoneCity
      ) = readDf(paths, incrDate, prevDate)

    //get  Customer CustomerSegments.getCustomerSegments
    val dfCustSegCalcIncr = CustomerSegments.getCustomerSegments(dfCustomerSegmentsIncr)
    //FK_CUSTOMER, MVP_TYPE, SEGMENT, DISCOUNT_SCORE

    //call SalesOrderAddress.processVariable
    val (dfSalesOrderAddrFavCalc, dfSalesOrderAddrFavFull) = SalesOrderAddress.processVariable(dfSalesOrderIncr, dfSalesOrderAddrFull, dfSalesOrderAddrFavPrevFull)
    // FK_CUSTOMER, CITY, MOBILE, FIRST_NAME, LAST_NAME

    val pathSalesOrderFavFull = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ORDER_ADDRESS, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(dfSalesOrderAddrFavFull, pathSalesOrderFavFull, saveMode)

    //call SalesOrder.processVariable for LAST_ORDER_DATE variable
    val dfSalesOrderCalcFull = SalesOrder.processVariables(dfSalesOrderCalcPrevFull, dfSalesOrderIncr)
    //FK_CUSTOMER, LAST_ORDER_DATE, UPDATED_AT, FIRST_ORDER_DATE, ORDERS_COUNT, DAYS_SINCE_LAST_ORDER

    val pathSalesOrderCalcFull = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ORDER, DataSets.DAILY_MODE, incrDate)
    DataWriter.writeParquet(dfSalesOrderCalcFull, pathSalesOrderCalcFull, saveMode)

    //SalesOrderItem.getSucessfulOrders for NET_ORDERS for variable
    val (dfSuccessfullOrders, successfulCalcFull) = SalesOrderItem.getSuccessfullOrders(dfSalesOrderItemIncr, dfSalesOrderFull, dfSalesOrderItemCalcPrevFull)
    //ORDERS_COUNT_SUCCESSFUL

    val pathSalesOrderItem = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ORDER_ITEM_ORDERS_COUNT, DataSets.DAILY_MODE, incrDate)
    DataWriter.writeParquet(successfulCalcFull, pathSalesOrderItem, saveMode)

    //Save Data Frame Contact List Mobile
    val (dfContactListMobileIncr, dfContactListMobileFull) = getContactListMobileDF (
      dfCustomerIncr,
      dfCustomerListMobilePrevFull,
      dfCustSegCalcIncr,
      dfNLSIncr,
      dfSalesOrderAddrFavCalc,
      dfSalesOrderCalcFull,
      dfSuccessfullOrders,
      dfDND,
      dfSmsOptOut,
      dfBlockedNumbers,
      dfZoneCity)

    val pathContactListMobileFull = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CONTACT_LIST_MOBILE, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(dfContactListMobileFull, pathContactListMobileFull, saveMode)

    val pathContactListMobile = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CONTACT_LIST_MOBILE, DataSets.DAILY_MODE, incrDate)
    DataWriter.writeParquet(dfContactListMobileIncr, pathContactListMobile, saveMode)

  }

  /**
   *
   * @param dfCustomerIncr Bob's customer table data for the yesterday's date
   * @param dfCustomerListMobilePrevFull Day Before yestreday's data for contact List mobile file.
   * @param dfCustSegCalcIncr
   * @param dfNLSIncr
   * @param dfSalesOrderAddrFavCalc
   * @param dfSalesOrderCalcFull
   * @param dfSuccessfullOrders
   * @param dfZoneCity
   * @return
   */
  def getContactListMobileDF(
    dfCustomerIncr: DataFrame,
    dfCustomerListMobilePrevFull: DataFrame,
    dfCustSegCalcIncr: DataFrame,
    dfNLSIncr: DataFrame,
    dfSalesOrderAddrFavCalc: DataFrame,
    dfSalesOrderCalcFull: DataFrame,
    dfSuccessfullOrders: DataFrame,
    dfDND: DataFrame,
    dfSmsOptOut: DataFrame,
    dfBlockedNumbers: DataFrame,
    dfZoneCity: DataFrame): (DataFrame, DataFrame) = {

    if (dfCustomerIncr == null || dfCustSegCalcIncr == null || dfNLSIncr == null) {
      log("Data frame should not be null")
      return null
    }

    if (!SchemaUtils.isSchemaEqual(dfCustomerIncr.schema, Schema.customer) ||
      !SchemaUtils.isSchemaEqual(dfCustSegCalcIncr.schema, Schema.customerSegments) ||
      !SchemaUtils.isSchemaEqual(dfNLSIncr.schema, Schema.nls)) {
      log("schema attributes or data type mismatch")
      return null
    }

    val nls = dfNLSIncr.select(
      col(NewsletterVariables.EMAIL) as NewsletterVariables.NLS_EMAIL,
      col(NewsletterVariables.STATUS),
      col(NewsletterVariables.UNSUBSCRIBE_KEY),
      col(NewsletterVariables.CREATED_AT) as NewsletterVariables.NLS_CREATED_AT,
      col(NewsletterVariables.UPDATED_AT) as NewsletterVariables.NLS_UPDATED_AT)

    val dfSmsOptOutMerged = dfSmsOptOut.select(DNDVariables.MOBILE_NUMBER).unionAll(dfBlockedNumbers.select(DNDVariables.MOBILE_NUMBER)).dropDuplicates()

    //Name of variable: CUSTOMERS PREFERRED ORDER TIMESLOT
    // val udfCPOT = SalesOrder.getCPOT(dfSalesOrderAddrFavCalc: DataFrame)

    val dfMergedIncr = mergeIncrData(dfCustomerIncr, dfCustSegCalcIncr, nls, dfSalesOrderAddrFavCalc, dfSalesOrderCalcFull, dfSuccessfullOrders, dfZoneCity, dfDND, dfSmsOptOutMerged)

    var dfFull: DataFrame = dfMergedIncr

    if (null != dfCustomerListMobilePrevFull) {

      //join old and new data frame
      val joinDF = MergeUtils.joinOldAndNewDF(dfMergedIncr, dfCustomerListMobilePrevFull, CustomerVariables.ID_CUSTOMER)

      //merge old and new data frame
      dfFull = joinDF.select(
        Udf.latestInt(joinDF(CustomerVariables.ID_CUSTOMER), joinDF(CustomerVariables.NEW_ + CustomerVariables.ID_CUSTOMER)) as CustomerVariables.UID,

        Udf.latestString(joinDF(CustomerVariables.EMAIL), joinDF(CustomerVariables.NEW_ + CustomerVariables.EMAIL)) as CustomerVariables.EMAIL,

        Udf.latestString(joinDF(CustomerVariables.EMAIL_SUBSCRIPTION_STATUS), joinDF(CustomerVariables.NEW_ + CustomerVariables.EMAIL_SUBSCRIPTION_STATUS)) as CustomerVariables.EMAIL_SUBSCRIPTION_STATUS,

        Udf.latestString(joinDF(CustomerVariables.PHONE), joinDF(CustomerVariables.NEW_ + CustomerVariables.PHONE)) as CustomerVariables.PHONE,

        coalesce(joinDF(CustomerVariables.NEW_ + CustomerVariables.MOBILE_PERMISSION_STATUS), joinDF(CustomerVariables.MOBILE_PERMISSION_STATUS)) as CustomerVariables.MOBILE_PERMISSION_STATUS, // Mobile Permission Status

        Udf.latestString(joinDF(CustomerVariables.CITY), joinDF(CustomerVariables.NEW_ + CustomerVariables.CITY)) as CustomerVariables.CITY,

        lit("IN") as CustomerVariables.COUNTRY,

        Udf.latestString(joinDF(CustomerVariables.FIRST_NAME), joinDF(CustomerVariables.NEW_ + CustomerVariables.FIRST_NAME)) as CustomerVariables.FIRST_NAME,

        Udf.latestString(joinDF(CustomerVariables.LAST_NAME), joinDF(CustomerVariables.NEW_ + CustomerVariables.LAST_NAME)) as CustomerVariables.LAST_NAME,

        Udf.latestDate(joinDF(CustomerVariables.BIRTHDAY), joinDF(CustomerVariables.NEW_ + CustomerVariables.BIRTHDAY)) as CustomerVariables.BIRTHDAY,

        coalesce(joinDF(CustomerVariables.NEW_ + CustomerSegmentsVariables.MVP_TYPE), joinDF(CustomerSegmentsVariables.MVP_TYPE)) as CustomerSegmentsVariables.MVP_TYPE,

        joinDF(CustomerVariables.NEW_ + SalesOrderItemVariables.NET_ORDERS) + joinDF(SalesOrderItemVariables.NET_ORDERS) as SalesOrderItemVariables.NET_ORDERS,

        coalesce(joinDF(CustomerVariables.NEW_ + SalesOrderVariables.LAST_ORDER_DATE), joinDF(SalesOrderVariables.LAST_ORDER_DATE)) as SalesOrderVariables.LAST_ORDER_DATE,

        Udf.latestString(joinDF(CustomerVariables.GENDER), joinDF(CustomerVariables.NEW_ + CustomerVariables.GENDER)) as CustomerVariables.GENDER,

        Udf.minTimestamp(joinDF(CustomerVariables.REG_DATE), joinDF(CustomerVariables.NEW_ + CustomerVariables.REG_DATE)) as CustomerVariables.REG_DATE,

        coalesce(joinDF(CustomerVariables.NEW_ + CustomerSegmentsVariables.SEGMENT), joinDF(CustomerSegmentsVariables.SEGMENT)) as CustomerSegmentsVariables.SEGMENT,

        Udf.latestInt(joinDF(CustomerVariables.AGE), joinDF(CustomerVariables.NEW_ + CustomerVariables.AGE)) as CustomerVariables.AGE,

        Udf.latestString(joinDF(CustomerVariables.REWARD_TYPE), joinDF(CustomerVariables.NEW_ + CustomerVariables.REWARD_TYPE)) as CustomerVariables.REWARD_TYPE,

        lit("") as CustomerVariables.IS_REFERRED, //IS_REFERRED

        Udf.latestTimestamp(joinDF(NewsletterVariables.NL_SUB_DATE), joinDF(CustomerVariables.NEW_ + NewsletterVariables.NL_SUB_DATE)) as NewsletterVariables.NL_SUB_DATE,

        Udf.latestBool(joinDF(CustomerVariables.VERIFICATION_STATUS), joinDF(CustomerVariables.NEW_ + CustomerVariables.VERIFICATION_STATUS)) as CustomerVariables.VERIFICATION_STATUS,

        Udf.maxTimestamp(joinDF(CustomerVariables.LAST_UPDATED_AT), joinDF(CustomerVariables.NEW_ + CustomerVariables.LAST_UPDATED_AT)) as CustomerVariables.LAST_UPDATED_AT,

        Udf.latestString(joinDF(NewsletterVariables.UNSUB_KEY), joinDF(CustomerVariables.NEW_ + NewsletterVariables.UNSUB_KEY)) as NewsletterVariables.UNSUB_KEY,

        coalesce(joinDF(CustomerVariables.NEW_ + CustomerVariables.CITY_TIER), joinDF(CustomerVariables.CITY_TIER)) as CustomerVariables.CITY_TIER,

        coalesce(joinDF(CustomerVariables.NEW_ + CustomerVariables.STATE_ZONE), joinDF(CustomerVariables.STATE_ZONE)) as CustomerVariables.STATE_ZONE,

        coalesce(joinDF(CustomerVariables.NEW_ + CustomerSegmentsVariables.DISCOUNT_SCORE), joinDF(CustomerSegmentsVariables.DISCOUNT_SCORE)) as CustomerSegmentsVariables.DISCOUNT_SCORE,

        coalesce(joinDF(CustomerVariables.NEW_ + CustomerVariables.DND), joinDF(CustomerVariables.DND)) as CustomerVariables.DND // DND
      )
    }

    (dfMergedIncr, dfFull)
  }

  def mergeIncrData(customerIncr: DataFrame, custSegCalcIncr: DataFrame, nls: DataFrame, salesAddrCalFull: DataFrame, salesOrderCalcFull: DataFrame, successfulOrdersIncr: DataFrame, cityZone: DataFrame, dnd: DataFrame, smsOptOut: DataFrame): DataFrame = {

    val customerSeg = customerIncr.join(custSegCalcIncr, customerIncr(CustomerVariables.ID_CUSTOMER) === custSegCalcIncr(CustomerSegmentsVariables.FK_CUSTOMER), SQL.FULL_OUTER)
      .select(
        coalesce(customerIncr(CustomerVariables.ID_CUSTOMER), custSegCalcIncr(CustomerSegmentsVariables.FK_CUSTOMER)) as CustomerVariables.ID_CUSTOMER,
        customerIncr(CustomerVariables.EMAIL),
        customerIncr(CustomerVariables.BIRTHDAY) as CustomerVariables.DOB,
        customerIncr(CustomerVariables.GENDER),
        customerIncr(CustomerVariables.REG_DATE),
        customerIncr(CustomerVariables.FIRST_NAME),
        customerIncr(CustomerVariables.LAST_NAME),
        customerIncr(CustomerVariables.PHONE),
        customerIncr(CustomerVariables.IS_CONFIRMED) as CustomerVariables.VERIFICATION_STATUS,
        Udf.age(customerIncr(CustomerVariables.BIRTHDAY)) as CustomerVariables.AGE,
        customerIncr(CustomerVariables.REWARD_TYPE) as CustomerVariables.PLATINUM_STATUS,
        customerIncr(CustomerVariables.UPDATED_AT),
        custSegCalcIncr(CustomerSegmentsVariables.MVP_TYPE),
        custSegCalcIncr(CustomerSegmentsVariables.SEGMENT),
        custSegCalcIncr(CustomerSegmentsVariables.DISCOUNT_SCORE))

    val customerMerged = customerSeg.join(nls, nls(NewsletterVariables.EMAIL) === customerSeg(CustomerVariables.EMAIL), SQL.FULL_OUTER)
      .select(
        coalesce(customerSeg(CustomerVariables.ID_CUSTOMER), nls(NewsletterVariables.FK_CUSTOMER)) as CustomerVariables.ID_CUSTOMER,
        customerSeg(CustomerVariables.EMAIL),
        customerSeg(CustomerVariables.DOB),
        customerSeg(CustomerVariables.GENDER),
        Udf.minTimestamp(customerSeg(CustomerVariables.CREATED_AT), nls(NewsletterVariables.NLS_CREATED_AT)) as CustomerVariables.REG_DATE,
        customerSeg(CustomerVariables.VERIFICATION_STATUS),
        customerSeg(CustomerVariables.AGE),
        customerSeg(CustomerVariables.FIRST_NAME),
        customerSeg(CustomerVariables.LAST_NAME),
        customerSeg(CustomerVariables.PHONE),
        customerSeg(CustomerVariables.PLATINUM_STATUS),
        customerSeg(CustomerSegmentsVariables.MVP_TYPE),
        customerSeg(CustomerSegmentsVariables.SEGMENT),
        customerSeg(CustomerSegmentsVariables.DISCOUNT_SCORE),
        udfEmailOptInStatus(nls(NewsletterVariables.NLS_EMAIL), nls(NewsletterVariables.STATUS)) as CustomerVariables.EMAIL_SUBSCRIPTION_STATUS,
        nls(NewsletterVariables.NLS_CREATED_AT) as NewsletterVariables.NL_SUB_DATE,
        nls(NewsletterVariables.UNSUBSCRIBE_KEY) as NewsletterVariables.UNSUB_KEY,
        Udf.maxTimestamp(customerSeg(CustomerVariables.UPDATED_AT), nls(NewsletterVariables.UPDATED_AT)) as CustomerVariables.UPDATED_AT)

    val salesOrderAddress = salesAddrCalFull.join(salesOrderCalcFull, salesAddrCalFull(SalesOrderVariables.FK_CUSTOMER) === salesOrderCalcFull(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
      .select(
        coalesce(salesAddrCalFull(SalesOrderVariables.FK_CUSTOMER), salesOrderCalcFull(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
        salesAddrCalFull(SalesAddressVariables.CITY),
        salesAddrCalFull(SalesAddressVariables.MOBILE),
        salesAddrCalFull(SalesAddressVariables.FIRST_NAME),
        salesAddrCalFull(SalesAddressVariables.LAST_NAME),
        salesOrderCalcFull(SalesOrderVariables.LAST_ORDER_DATE),
        salesOrderCalcFull(SalesOrderVariables.UPDATED_AT))

    val salesMerged = salesOrderAddress.join(successfulOrdersIncr, successfulOrdersIncr(SalesOrderVariables.FK_CUSTOMER) === salesOrderAddress(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
      .select(
        coalesce(salesOrderAddress(SalesOrderVariables.FK_CUSTOMER), successfulOrdersIncr(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
        salesOrderAddress(SalesAddressVariables.CITY),
        salesOrderAddress(SalesAddressVariables.MOBILE),
        salesOrderAddress(SalesAddressVariables.FIRST_NAME),
        salesOrderAddress(SalesAddressVariables.LAST_NAME),
        salesOrderAddress(SalesOrderVariables.LAST_ORDER_DATE),
        salesOrderAddress(SalesOrderVariables.UPDATED_AT),
        successfulOrdersIncr(SalesOrderItemVariables.ORDERS_COUNT_SUCCESSFUL))

    val mergedIncr = customerMerged.join(salesMerged, salesMerged(SalesOrderVariables.FK_CUSTOMER) === customerMerged(CustomerVariables.ID_CUSTOMER))
      .select(
        coalesce(customerMerged(SalesOrderVariables.FK_CUSTOMER), salesMerged(CustomerVariables.ID_CUSTOMER)) as CustomerVariables.ID_CUSTOMER,
        customerMerged(CustomerVariables.EMAIL),
        customerMerged(CustomerVariables.DOB),
        customerMerged(CustomerVariables.GENDER),
        customerMerged(CustomerVariables.REG_DATE),
        customerMerged(CustomerVariables.VERIFICATION_STATUS),
        customerMerged(CustomerVariables.AGE),
        customerMerged(CustomerVariables.PLATINUM_STATUS),
        customerMerged(CustomerSegmentsVariables.MVP_TYPE),
        customerMerged(CustomerSegmentsVariables.SEGMENT),
        customerMerged(CustomerSegmentsVariables.DISCOUNT_SCORE),
        customerMerged(CustomerVariables.EMAIL_SUBSCRIPTION_STATUS),
        customerMerged(NewsletterVariables.NL_SUB_DATE),
        customerMerged(NewsletterVariables.UNSUB_KEY),
        salesMerged(SalesAddressVariables.CITY),
        coalesce(customerMerged(CustomerVariables.FIRST_NAME), salesMerged(SalesAddressVariables.FIRST_NAME)) as CustomerVariables.FIRST_NAME,
        coalesce(customerMerged(CustomerVariables.LAST_NAME), salesMerged(SalesAddressVariables.LAST_NAME)) as CustomerVariables.LAST_NAME,
        coalesce(customerMerged(CustomerVariables.PHONE), salesMerged(SalesAddressVariables.MOBILE)) as SalesAddressVariables.MOBILE,
        salesMerged(SalesOrderVariables.LAST_ORDER_DATE),
        Udf.maxTimestamp(salesMerged(SalesOrderVariables.UPDATED_AT), customerMerged(CustomerVariables.UPDATED_AT)) as CustomerVariables.LAST_UPDATED_AT,
        salesMerged(SalesOrderItemVariables.ORDERS_COUNT_SUCCESSFUL) as SalesOrderItemVariables.NET_ORDERS)

    val cityBc = Spark.getContext().broadcast(cityZone).value

    val cityJoined = mergedIncr.join(cityBc, Udf.toLowercase(cityBc(SalesAddressVariables.CITY)) === Udf.toLowercase(mergedIncr(SalesAddressVariables.CITY)), SQL.LEFT_OUTER)
      .select(
        mergedIncr(SalesOrderVariables.FK_CUSTOMER),
        mergedIncr(CustomerVariables.EMAIL),
        mergedIncr(CustomerVariables.DOB),
        mergedIncr(CustomerVariables.GENDER),
        mergedIncr(CustomerVariables.REG_DATE),
        mergedIncr(CustomerVariables.VERIFICATION_STATUS),
        mergedIncr(CustomerVariables.AGE),
        mergedIncr(CustomerVariables.PLATINUM_STATUS),
        mergedIncr(CustomerSegmentsVariables.MVP_TYPE),
        mergedIncr(CustomerSegmentsVariables.SEGMENT),
        mergedIncr(CustomerSegmentsVariables.DISCOUNT_SCORE),
        mergedIncr(CustomerVariables.EMAIL_SUBSCRIPTION_STATUS),
        mergedIncr(NewsletterVariables.NL_SUB_DATE),
        mergedIncr(NewsletterVariables.UNSUB_KEY),
        mergedIncr(SalesAddressVariables.CITY),
        mergedIncr(SalesAddressVariables.FIRST_NAME),
        mergedIncr(CustomerVariables.LAST_NAME),
        mergedIncr(CustomerVariables.PHONE),
        mergedIncr(SalesOrderVariables.LAST_ORDER_DATE),
        mergedIncr(CustomerVariables.LAST_UPDATED_AT),
        mergedIncr(SalesOrderItemVariables.NET_ORDERS),
        cityBc(CustomerVariables.ZONE) as CustomerVariables.STATE_ZONE,
        cityBc(CustomerVariables.TIER1) as CustomerVariables.CITY_TIER)

    val dndBc = Spark.getContext().broadcast(dnd).value

    val dndMerged = cityJoined.join(dndBc, dndBc(DNDVariables.MOBILE_NUMBER) === cityJoined(CustomerVariables.PHONE), SQL.LEFT_OUTER)
      .select(
        cityJoined(SalesOrderVariables.FK_CUSTOMER),
        cityJoined(CustomerVariables.EMAIL),
        cityJoined(CustomerVariables.DOB),
        cityJoined(CustomerVariables.GENDER),
        cityJoined(CustomerVariables.REG_DATE),
        cityJoined(CustomerVariables.VERIFICATION_STATUS),
        cityJoined(CustomerVariables.AGE),
        cityJoined(CustomerVariables.PLATINUM_STATUS),
        cityJoined(CustomerSegmentsVariables.MVP_TYPE),
        cityJoined(CustomerSegmentsVariables.SEGMENT),
        cityJoined(CustomerSegmentsVariables.DISCOUNT_SCORE),
        cityJoined(CustomerVariables.EMAIL_SUBSCRIPTION_STATUS),
        cityJoined(NewsletterVariables.NL_SUB_DATE),
        cityJoined(NewsletterVariables.UNSUB_KEY),
        cityJoined(SalesAddressVariables.CITY),
        cityJoined(SalesAddressVariables.FIRST_NAME),
        cityJoined(CustomerVariables.LAST_NAME),
        cityJoined(CustomerVariables.PHONE),
        cityJoined(SalesOrderVariables.LAST_ORDER_DATE),
        cityJoined(CustomerVariables.LAST_UPDATED_AT),
        cityJoined(SalesOrderItemVariables.NET_ORDERS),
        cityJoined(CustomerVariables.STATE_ZONE),
        cityJoined(CustomerVariables.CITY_TIER),
        when(dndBc(DNDVariables.MOBILE_NUMBER).!==(null), "1").otherwise("0") as CustomerVariables.DND)

    val smsBc = Spark.getContext().broadcast(smsOptOut).value

    val res = dndMerged.join(smsBc, dndMerged(DNDVariables.MOBILE_NUMBER) === smsBc(DNDVariables.MOBILE_NUMBER), SQL.LEFT_OUTER)
      .select(
        dndMerged(SalesOrderVariables.FK_CUSTOMER),
        dndMerged(CustomerVariables.EMAIL),
        dndMerged(CustomerVariables.DOB),
        dndMerged(CustomerVariables.GENDER),
        dndMerged(CustomerVariables.REG_DATE),
        dndMerged(CustomerVariables.VERIFICATION_STATUS),
        dndMerged(CustomerVariables.AGE),
        dndMerged(CustomerVariables.PLATINUM_STATUS),
        dndMerged(CustomerSegmentsVariables.MVP_TYPE),
        dndMerged(CustomerSegmentsVariables.SEGMENT),
        dndMerged(CustomerSegmentsVariables.DISCOUNT_SCORE),
        dndMerged(CustomerVariables.EMAIL_SUBSCRIPTION_STATUS),
        dndMerged(NewsletterVariables.NL_SUB_DATE),
        dndMerged(NewsletterVariables.UNSUB_KEY),
        dndMerged(SalesAddressVariables.CITY),
        dndMerged(SalesAddressVariables.FIRST_NAME),
        dndMerged(CustomerVariables.LAST_NAME),
        dndMerged(CustomerVariables.PHONE),
        dndMerged(SalesOrderVariables.LAST_ORDER_DATE),
        dndMerged(CustomerVariables.LAST_UPDATED_AT),
        dndMerged(SalesOrderItemVariables.NET_ORDERS),
        dndMerged(CustomerVariables.STATE_ZONE),
        dndMerged(CustomerVariables.CITY_TIER),
        dndMerged(CustomerVariables.DND),
        when(smsBc(DNDVariables.MOBILE_NUMBER).!==(null), "o").otherwise("i") as CustomerVariables.MOBILE_PERMISSION_STATUS
      )

    return res
  }

  /**
   * read Data Frames
   * @param incrDate
   * @return
   */
  def readDf(incrDate: String, prevDate: String): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {

    val dfCustomerIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CUSTOMER, DataSets.DAILY_MODE, incrDate)
    val dfCustomerListMobilePrevFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CONTACT_LIST_MOBILE, DataSets.FULL_MERGE_MODE, prevDate)

    val dfCustomerSegmentsIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CUSTOMER_SEGMENTS, DataSets.DAILY_MODE, incrDate)
    val dfNLSIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.NEWSLETTER_SUBSCRIPTION, DataSets.DAILY_MODE, incrDate)
    val dfSalesOrderIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.DAILY_MODE, incrDate)
    val dfSalesOrderFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.FULL_MERGE_MODE, incrDate)
    val dfSalesOrderAddrFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ADDRESS, DataSets.FULL_MERGE_MODE, incrDate)

    val dfSalesOrderAddrFavPrevFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ORDER_ADDRESS, DataSets.FULL_MERGE_MODE, prevDate)

    val dfSalesOrderItemIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ITEM, DataSets.DAILY_MODE, incrDate)

    val dfSalesOrderCalcPrevFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ORDER, DataSets.FULL_MERGE_MODE, prevDate)

    val dfSalesOrderItemCalcPrevFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ORDER_ITEM_ORDERS_COUNT, DataSets.FULL_MERGE_MODE, prevDate)

    val dfDND = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.RESPONSYS, DataSets.DND, DataSets.DAILY_MODE, incrDate)

    val dfSmsOptOut = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.SMS_OPT_OUT, DataSets.RESPONSYS, DataSets.FULL, incrDate)

    val dfBlockedNumbers = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.SMS_OPT_OUT, DataSets.SOLUTIONS_INFINITI, DataSets.FULL, incrDate)

    val dfZoneCity = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, DataSets.ZONE_CITY, DataSets.DAILY_MODE, incrDate)
    //TODO store the city names in lower case, all data coming as Upper case
    (
      dfCustomerIncr,
      dfCustomerListMobilePrevFull,
      dfCustomerSegmentsIncr,
      dfNLSIncr,
      dfSalesOrderIncr,
      dfSalesOrderFull,
      dfSalesOrderAddrFull,
      dfSalesOrderAddrFavPrevFull,
      dfSalesOrderItemIncr,
      dfSalesOrderCalcPrevFull,
      dfSalesOrderItemCalcPrevFull,
      dfDND,
      dfSmsOptOut,
      dfBlockedNumbers,
      dfZoneCity)
  }

  def readDf(paths: String, incrDate: String, prevDate: String): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {
    if (null != paths) {
      val pathList = paths.split(";")
      val custPath = pathList(0)
      val custSegPath = pathList(1)
      val nlsPath = pathList(2)
      val salesOrderItemPath = pathList(3)

      val dfCustomerIncr = DataReader.getDataFrame4mFullPath(custPath, DataSets.PARQUET)
      val dfCustomerSegmentsIncr = DataReader.getDataFrame4mFullPath(custSegPath, DataSets.PARQUET)
      val dfNLSIncr = DataReader.getDataFrame4mFullPath(nlsPath, DataSets.PARQUET)
      val dfSalesOrderFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.FULL_MERGE_MODE, incrDate)
      val dfSalesOrderIncr = dfSalesOrderFull
      val dfSalesOrderAddrFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ADDRESS, DataSets.FULL_MERGE_MODE, incrDate)

      val dfSalesOrderItemIncr = DataReader.getDataFrame4mFullPath(salesOrderItemPath, DataSets.PARQUET)

      val dfDND = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, DataSets.DND, DataSets.FULL, incrDate)

      val dfSmsOptOut = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.SMS_OPT_OUT, DataSets.RESPONSYS, DataSets.FULL, incrDate)

      val dfBlockedNumbers = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.SMS_OPT_OUT, DataSets.SOLUTIONS_INFINITI, DataSets.FULL, incrDate)

      val dfZoneCity = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, DataSets.ZONE_CITY, DataSets.DAILY_MODE, incrDate)

      (
        dfCustomerIncr,
        null,
        dfCustomerSegmentsIncr,
        dfNLSIncr,
        dfSalesOrderIncr,
        dfSalesOrderFull,
        dfSalesOrderAddrFull,
        null,
        dfSalesOrderItemIncr,
        null,
        null,
        dfDND,
        dfSmsOptOut,
        dfBlockedNumbers,
        dfZoneCity)
    } else {
      readDf(incrDate, prevDate)
    }
  }

}