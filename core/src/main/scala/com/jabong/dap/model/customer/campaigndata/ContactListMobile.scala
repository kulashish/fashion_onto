package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.ContactListMobileVars
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

  val SUM_SPECIAL_PRICE = "sum_special_price"
  val COUNT_BRAND = "count_brand"
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
      dfZoneCity,
      dfYestItr
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

    val dfMostPreferredBrand = getdfMostPreferredBrand(dfSalesOrderFull, dfSalesOrderItemCalcPrevFull, dfYestItr)

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
      dfZoneCity,
      dfMostPreferredBrand)

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
    dfZoneCity: DataFrame,
    dfMostPreferredBrand: DataFrame): (DataFrame, DataFrame) = {

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

    val dfMergedIncr = mergeIncrData(dfCustomerIncr, dfCustSegCalcIncr, nls, dfSalesOrderAddrFavCalc, dfSalesOrderCalcFull, dfSuccessfullOrders, dfZoneCity, dfDND, dfSmsOptOut, dfMostPreferredBrand)

    var dfFull: DataFrame = dfMergedIncr

    if (null != dfCustomerListMobilePrevFull) {

      //join old and new data frame
      val joinDF = MergeUtils.joinOldAndNewDF(dfMergedIncr, dfCustomerListMobilePrevFull, CustomerVariables.ID_CUSTOMER)

      //merge old and new data frame
      dfFull = joinDF.select(
        Udf.latestInt(joinDF(CustomerVariables.ID_CUSTOMER), joinDF(CustomerVariables.NEW_ + CustomerVariables.ID_CUSTOMER)) as ContactListMobileVars.UID,

        Udf.latestString(joinDF(CustomerVariables.EMAIL), joinDF(CustomerVariables.NEW_ + CustomerVariables.EMAIL)) as CustomerVariables.EMAIL,

        Udf.latestString(joinDF(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS), joinDF(CustomerVariables.NEW_ + ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS)) as ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS,

        Udf.latestString(joinDF(CustomerVariables.PHONE), joinDF(CustomerVariables.NEW_ + CustomerVariables.PHONE)) as CustomerVariables.PHONE,

        coalesce(joinDF(CustomerVariables.NEW_ + ContactListMobileVars.MOBILE_PERMISSION_STATUS), joinDF(ContactListMobileVars.MOBILE_PERMISSION_STATUS)) as ContactListMobileVars.MOBILE_PERMISSION_STATUS, // Mobile Permission Status

        Udf.latestString(joinDF(CustomerVariables.CITY), joinDF(CustomerVariables.NEW_ + CustomerVariables.CITY)) as CustomerVariables.CITY,

        lit("IN") as ContactListMobileVars.COUNTRY,

        Udf.latestString(joinDF(CustomerVariables.FIRST_NAME), joinDF(CustomerVariables.NEW_ + CustomerVariables.FIRST_NAME)) as CustomerVariables.FIRST_NAME,

        Udf.latestString(joinDF(CustomerVariables.LAST_NAME), joinDF(CustomerVariables.NEW_ + CustomerVariables.LAST_NAME)) as CustomerVariables.LAST_NAME,

        Udf.latestDate(joinDF(CustomerVariables.BIRTHDAY), joinDF(CustomerVariables.NEW_ + CustomerVariables.BIRTHDAY)) as CustomerVariables.BIRTHDAY,

        coalesce(joinDF(CustomerVariables.NEW_ + ContactListMobileVars.MVP_TYPE), joinDF(ContactListMobileVars.MVP_TYPE)) as ContactListMobileVars.MVP_TYPE,

        joinDF(CustomerVariables.NEW_ + ContactListMobileVars.NET_ORDERS) + joinDF(ContactListMobileVars.NET_ORDERS) as ContactListMobileVars.NET_ORDERS,

        coalesce(joinDF(CustomerVariables.NEW_ + ContactListMobileVars.LAST_ORDER_DATE), joinDF(ContactListMobileVars.LAST_ORDER_DATE)) as ContactListMobileVars.LAST_ORDER_DATE,

        Udf.latestString(joinDF(CustomerVariables.GENDER), joinDF(CustomerVariables.NEW_ + CustomerVariables.GENDER)) as CustomerVariables.GENDER,

        Udf.minTimestamp(joinDF(ContactListMobileVars.REG_DATE), joinDF(CustomerVariables.NEW_ + ContactListMobileVars.REG_DATE)) as ContactListMobileVars.REG_DATE,

        coalesce(joinDF(CustomerVariables.NEW_ + CustomerSegmentsVariables.SEGMENT), joinDF(CustomerSegmentsVariables.SEGMENT)) as CustomerSegmentsVariables.SEGMENT,

        Udf.latestInt(joinDF(ContactListMobileVars.AGE), joinDF(CustomerVariables.NEW_ + ContactListMobileVars.AGE)) as ContactListMobileVars.AGE,

        Udf.latestString(joinDF(CustomerVariables.REWARD_TYPE), joinDF(CustomerVariables.NEW_ + CustomerVariables.REWARD_TYPE)) as CustomerVariables.REWARD_TYPE,

        lit("") as ContactListMobileVars.IS_REFERRED, //IS_REFERRED

        Udf.latestTimestamp(joinDF(ContactListMobileVars.NL_SUB_DATE), joinDF(CustomerVariables.NEW_ + ContactListMobileVars.NL_SUB_DATE)) as ContactListMobileVars.NL_SUB_DATE,

        Udf.latestBool(joinDF(ContactListMobileVars.VERIFICATION_STATUS), joinDF(CustomerVariables.NEW_ + ContactListMobileVars.VERIFICATION_STATUS)) as ContactListMobileVars.VERIFICATION_STATUS,

        Udf.maxTimestamp(joinDF(CustomerVariables.LAST_UPDATED_AT), joinDF(CustomerVariables.NEW_ + CustomerVariables.LAST_UPDATED_AT)) as CustomerVariables.LAST_UPDATED_AT,

        Udf.latestString(joinDF(ContactListMobileVars.UNSUB_KEY), joinDF(CustomerVariables.NEW_ + ContactListMobileVars.UNSUB_KEY)) as ContactListMobileVars.UNSUB_KEY,

        coalesce(joinDF(CustomerVariables.NEW_ + ContactListMobileVars.CITY_TIER), joinDF(ContactListMobileVars.CITY_TIER)) as ContactListMobileVars.CITY_TIER,

        coalesce(joinDF(CustomerVariables.NEW_ + ContactListMobileVars.STATE_ZONE), joinDF(ContactListMobileVars.STATE_ZONE)) as ContactListMobileVars.STATE_ZONE,

        coalesce(joinDF(CustomerVariables.NEW_ + CustomerSegmentsVariables.DISCOUNT_SCORE), joinDF(CustomerSegmentsVariables.DISCOUNT_SCORE)) as CustomerSegmentsVariables.DISCOUNT_SCORE,

        coalesce(joinDF(CustomerVariables.NEW_ + ContactListMobileVars.DND), joinDF(ContactListMobileVars.DND)) as ContactListMobileVars.DND, // DND
        coalesce(joinDF(CustomerVariables.NEW_ + ProductVariables.BRAND), joinDF(ProductVariables.BRAND)) as ProductVariables.BRAND
      )
    }

    (dfMergedIncr, dfFull)
  }

  def mergeIncrData(customerIncr: DataFrame, custSegCalcIncr: DataFrame, nls: DataFrame, salesAddrCalFull: DataFrame, salesOrderCalcFull: DataFrame, successfulOrdersIncr: DataFrame, cityZone: DataFrame, dnd: DataFrame, smsOptOut: DataFrame, dfMostPreferredBrand: DataFrame): DataFrame = {

    val customerAddBrandCol = customerIncr.join(dfMostPreferredBrand, customerIncr(CustomerVariables.ID_CUSTOMER) === dfMostPreferredBrand(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
      .select(
        customerIncr(CustomerVariables.ID_CUSTOMER),
        customerIncr(CustomerVariables.EMAIL),
        customerIncr(CustomerVariables.BIRTHDAY) as ContactListMobileVars.DOB,
        customerIncr(CustomerVariables.GENDER),
        customerIncr(ContactListMobileVars.REG_DATE),
        customerIncr(CustomerVariables.FIRST_NAME),
        customerIncr(CustomerVariables.LAST_NAME),
        customerIncr(CustomerVariables.PHONE),
        customerIncr(CustomerVariables.IS_CONFIRMED) as ContactListMobileVars.VERIFICATION_STATUS,
        Udf.age(customerIncr(CustomerVariables.BIRTHDAY)) as ContactListMobileVars.AGE,
        customerIncr(CustomerVariables.REWARD_TYPE) as ContactListMobileVars.PLATINUM_STATUS,
        customerIncr(CustomerVariables.UPDATED_AT),
        dfMostPreferredBrand(ProductVariables.BRAND))

    val customerSeg = customerAddBrandCol.join(custSegCalcIncr, customerAddBrandCol(CustomerVariables.ID_CUSTOMER) === custSegCalcIncr(CustomerSegmentsVariables.FK_CUSTOMER), SQL.FULL_OUTER)
      .select(
        coalesce(customerAddBrandCol(CustomerVariables.ID_CUSTOMER), custSegCalcIncr(CustomerSegmentsVariables.FK_CUSTOMER)) as CustomerVariables.ID_CUSTOMER,
        customerAddBrandCol(CustomerVariables.EMAIL),
        customerAddBrandCol(CustomerVariables.BIRTHDAY) as ContactListMobileVars.DOB,
        customerAddBrandCol(CustomerVariables.GENDER),
        customerAddBrandCol(ContactListMobileVars.REG_DATE),
        customerAddBrandCol(CustomerVariables.FIRST_NAME),
        customerAddBrandCol(CustomerVariables.LAST_NAME),
        customerAddBrandCol(CustomerVariables.PHONE),
        customerAddBrandCol(CustomerVariables.IS_CONFIRMED) as ContactListMobileVars.VERIFICATION_STATUS,
        Udf.age(customerAddBrandCol(CustomerVariables.BIRTHDAY)) as ContactListMobileVars.AGE,
        customerAddBrandCol(CustomerVariables.REWARD_TYPE) as ContactListMobileVars.PLATINUM_STATUS,
        customerAddBrandCol(CustomerVariables.UPDATED_AT),
        custSegCalcIncr(ContactListMobileVars.MVP_TYPE),
        custSegCalcIncr(CustomerSegmentsVariables.SEGMENT),
        custSegCalcIncr(CustomerSegmentsVariables.DISCOUNT_SCORE))

    val customerMerged = customerSeg.join(nls, nls(NewsletterVariables.EMAIL) === customerSeg(CustomerVariables.EMAIL), SQL.FULL_OUTER)
      .select(
        coalesce(customerSeg(CustomerVariables.ID_CUSTOMER), nls(NewsletterVariables.FK_CUSTOMER)) as CustomerVariables.ID_CUSTOMER,
        customerSeg(CustomerVariables.EMAIL),
        customerSeg(ContactListMobileVars.DOB),
        customerSeg(CustomerVariables.GENDER),
        Udf.minTimestamp(customerSeg(CustomerVariables.CREATED_AT), nls(NewsletterVariables.NLS_CREATED_AT)) as ContactListMobileVars.REG_DATE,
        customerSeg(ContactListMobileVars.VERIFICATION_STATUS),
        customerSeg(ContactListMobileVars.AGE),
        customerSeg(CustomerVariables.FIRST_NAME),
        customerSeg(CustomerVariables.LAST_NAME),
        customerSeg(CustomerVariables.PHONE),
        customerSeg(ContactListMobileVars.PLATINUM_STATUS),
        customerSeg(ContactListMobileVars.MVP_TYPE),
        customerSeg(CustomerSegmentsVariables.SEGMENT),
        customerSeg(CustomerSegmentsVariables.DISCOUNT_SCORE),
        udfEmailOptInStatus(nls(NewsletterVariables.NLS_EMAIL), nls(NewsletterVariables.STATUS)) as ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS,
        nls(NewsletterVariables.NLS_CREATED_AT) as ContactListMobileVars.NL_SUB_DATE,
        nls(NewsletterVariables.UNSUBSCRIBE_KEY) as ContactListMobileVars.UNSUB_KEY,
        Udf.maxTimestamp(customerSeg(CustomerVariables.UPDATED_AT), nls(NewsletterVariables.UPDATED_AT)) as CustomerVariables.UPDATED_AT)

    val salesOrderAddress = salesAddrCalFull.join(salesOrderCalcFull, salesAddrCalFull(SalesOrderVariables.FK_CUSTOMER) === salesOrderCalcFull(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
      .select(
        coalesce(salesAddrCalFull(SalesOrderVariables.FK_CUSTOMER), salesOrderCalcFull(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
        salesAddrCalFull(SalesAddressVariables.CITY),
        salesAddrCalFull(SalesAddressVariables.MOBILE),
        salesAddrCalFull(SalesAddressVariables.FIRST_NAME),
        salesAddrCalFull(SalesAddressVariables.LAST_NAME),
        salesOrderCalcFull(ContactListMobileVars.LAST_ORDER_DATE),
        salesOrderCalcFull(SalesOrderVariables.UPDATED_AT))

    val salesMerged = salesOrderAddress.join(successfulOrdersIncr, successfulOrdersIncr(SalesOrderVariables.FK_CUSTOMER) === salesOrderAddress(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
      .select(
        coalesce(salesOrderAddress(SalesOrderVariables.FK_CUSTOMER), successfulOrdersIncr(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
        salesOrderAddress(SalesAddressVariables.CITY),
        salesOrderAddress(SalesAddressVariables.MOBILE),
        salesOrderAddress(SalesAddressVariables.FIRST_NAME),
        salesOrderAddress(SalesAddressVariables.LAST_NAME),
        salesOrderAddress(ContactListMobileVars.LAST_ORDER_DATE),
        salesOrderAddress(SalesOrderVariables.UPDATED_AT),
        successfulOrdersIncr(SalesOrderItemVariables.ORDERS_COUNT_SUCCESSFUL))

    val mergedIncr = customerMerged.join(salesMerged, salesMerged(SalesOrderVariables.FK_CUSTOMER) === customerMerged(CustomerVariables.ID_CUSTOMER))
      .select(
        coalesce(customerMerged(SalesOrderVariables.FK_CUSTOMER), salesMerged(CustomerVariables.ID_CUSTOMER)) as CustomerVariables.ID_CUSTOMER,
        customerMerged(CustomerVariables.EMAIL),
        customerMerged(ContactListMobileVars.DOB),
        customerMerged(CustomerVariables.GENDER),
        customerMerged(ContactListMobileVars.REG_DATE),
        customerMerged(ContactListMobileVars.VERIFICATION_STATUS),
        customerMerged(ContactListMobileVars.AGE),
        customerMerged(ContactListMobileVars.PLATINUM_STATUS),
        customerMerged(ContactListMobileVars.MVP_TYPE),
        customerMerged(CustomerSegmentsVariables.SEGMENT),
        customerMerged(CustomerSegmentsVariables.DISCOUNT_SCORE),
        customerMerged(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS),
        customerMerged(ContactListMobileVars.NL_SUB_DATE),
        customerMerged(ContactListMobileVars.UNSUB_KEY),
        salesMerged(SalesAddressVariables.CITY),
        coalesce(customerMerged(CustomerVariables.FIRST_NAME), salesMerged(SalesAddressVariables.FIRST_NAME)) as CustomerVariables.FIRST_NAME,
        coalesce(customerMerged(CustomerVariables.LAST_NAME), salesMerged(SalesAddressVariables.LAST_NAME)) as CustomerVariables.LAST_NAME,
        coalesce(customerMerged(CustomerVariables.PHONE), salesMerged(SalesAddressVariables.MOBILE)) as SalesAddressVariables.MOBILE,
        salesMerged(ContactListMobileVars.LAST_ORDER_DATE),
        Udf.maxTimestamp(salesMerged(SalesOrderVariables.UPDATED_AT), customerMerged(CustomerVariables.UPDATED_AT)) as CustomerVariables.LAST_UPDATED_AT,
        salesMerged(SalesOrderItemVariables.ORDERS_COUNT_SUCCESSFUL) as ContactListMobileVars.NET_ORDERS)

    val cityBc = Spark.getContext().broadcast(cityZone).value

    val cityJoined = mergedIncr.join(cityBc, Udf.toLowercase(cityBc(SalesAddressVariables.CITY)) === Udf.toLowercase(mergedIncr(SalesAddressVariables.CITY)), SQL.LEFT_OUTER)
      .select(
        mergedIncr(SalesOrderVariables.FK_CUSTOMER),
        mergedIncr(CustomerVariables.EMAIL),
        mergedIncr(ContactListMobileVars.DOB),
        mergedIncr(CustomerVariables.GENDER),
        mergedIncr(ContactListMobileVars.REG_DATE),
        mergedIncr(ContactListMobileVars.VERIFICATION_STATUS),
        mergedIncr(ContactListMobileVars.AGE),
        mergedIncr(ContactListMobileVars.PLATINUM_STATUS),
        mergedIncr(ContactListMobileVars.MVP_TYPE),
        mergedIncr(CustomerSegmentsVariables.SEGMENT),
        mergedIncr(CustomerSegmentsVariables.DISCOUNT_SCORE),
        mergedIncr(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS),
        mergedIncr(ContactListMobileVars.NL_SUB_DATE),
        mergedIncr(ContactListMobileVars.UNSUB_KEY),
        mergedIncr(SalesAddressVariables.CITY),
        mergedIncr(SalesAddressVariables.FIRST_NAME),
        mergedIncr(CustomerVariables.LAST_NAME),
        mergedIncr(CustomerVariables.PHONE),
        mergedIncr(ContactListMobileVars.LAST_ORDER_DATE),
        mergedIncr(CustomerVariables.LAST_UPDATED_AT),
        mergedIncr(ContactListMobileVars.NET_ORDERS),
        cityBc(CustomerVariables.ZONE) as ContactListMobileVars.STATE_ZONE,
        cityBc(CustomerVariables.TIER1) as ContactListMobileVars.CITY_TIER)

    val dndBc = Spark.getContext().broadcast(dnd).value

    val dndMerged = cityJoined.join(dndBc, dndBc(DNDVariables.MOBILE_NUMBER) === cityJoined(CustomerVariables.PHONE), SQL.LEFT_OUTER)
      .select(
        cityJoined(SalesOrderVariables.FK_CUSTOMER),
        cityJoined(CustomerVariables.EMAIL),
        cityJoined(ContactListMobileVars.DOB),
        cityJoined(CustomerVariables.GENDER),
        cityJoined(ContactListMobileVars.REG_DATE),
        cityJoined(ContactListMobileVars.VERIFICATION_STATUS),
        cityJoined(ContactListMobileVars.AGE),
        cityJoined(ContactListMobileVars.PLATINUM_STATUS),
        cityJoined(ContactListMobileVars.MVP_TYPE),
        cityJoined(CustomerSegmentsVariables.SEGMENT),
        cityJoined(CustomerSegmentsVariables.DISCOUNT_SCORE),
        cityJoined(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS),
        cityJoined(ContactListMobileVars.NL_SUB_DATE),
        cityJoined(ContactListMobileVars.UNSUB_KEY),
        cityJoined(SalesAddressVariables.CITY),
        cityJoined(SalesAddressVariables.FIRST_NAME),
        cityJoined(CustomerVariables.LAST_NAME),
        cityJoined(CustomerVariables.PHONE),
        cityJoined(ContactListMobileVars.LAST_ORDER_DATE),
        cityJoined(CustomerVariables.LAST_UPDATED_AT),
        cityJoined(ContactListMobileVars.NET_ORDERS),
        cityJoined(ContactListMobileVars.STATE_ZONE),
        cityJoined(ContactListMobileVars.CITY_TIER),
        when(dndBc(DNDVariables.MOBILE_NUMBER).!==(null), "1").otherwise("0") as ContactListMobileVars.DND)

    val smsBc = Spark.getContext().broadcast(smsOptOut).value

    val res = dndMerged.join(smsBc, dndMerged(DNDVariables.MOBILE_NUMBER) === smsBc(DNDVariables.MOBILE_NUMBER), SQL.LEFT_OUTER)
      .select(
        dndMerged(SalesOrderVariables.FK_CUSTOMER),
        dndMerged(CustomerVariables.EMAIL),
        dndMerged(ContactListMobileVars.DOB),
        dndMerged(CustomerVariables.GENDER),
        dndMerged(ContactListMobileVars.REG_DATE),
        dndMerged(ContactListMobileVars.VERIFICATION_STATUS),
        dndMerged(ContactListMobileVars.AGE),
        dndMerged(ContactListMobileVars.PLATINUM_STATUS),
        dndMerged(ContactListMobileVars.MVP_TYPE),
        dndMerged(CustomerSegmentsVariables.SEGMENT),
        dndMerged(CustomerSegmentsVariables.DISCOUNT_SCORE),
        dndMerged(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS),
        dndMerged(ContactListMobileVars.NL_SUB_DATE),
        dndMerged(ContactListMobileVars.UNSUB_KEY),
        dndMerged(SalesAddressVariables.CITY),
        dndMerged(SalesAddressVariables.FIRST_NAME),
        dndMerged(CustomerVariables.LAST_NAME),
        dndMerged(CustomerVariables.PHONE),
        dndMerged(ContactListMobileVars.LAST_ORDER_DATE),
        dndMerged(CustomerVariables.LAST_UPDATED_AT),
        dndMerged(ContactListMobileVars.NET_ORDERS),
        dndMerged(ContactListMobileVars.STATE_ZONE),
        dndMerged(ContactListMobileVars.CITY_TIER),
        dndMerged(ContactListMobileVars.DND),
        when(smsBc(DNDVariables.MOBILE_NUMBER).!==(null), "o").otherwise("i") as ContactListMobileVars.MOBILE_PERMISSION_STATUS
      )

    return res
  }

  /**
   * read Data Frames
   * @param incrDate
   * @return
   */
  def readDf(incrDate: String, prevDate: String): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {

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

    val yestItr = CampaignInput.loadYesterdayItrSimpleData()

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
      dfZoneCity,
      yestItr)
  }

  def readDf(paths: String, incrDate: String, prevDate: String): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {
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

      val dfDND = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.RESPONSYS, DataSets.DND, DataSets.FULL_MERGE_MODE, incrDate)

      val dfSmsOptOut = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.SMS_OPT_OUT, DataSets.RESPONSYS, DataSets.FULL_MERGE_MODE, incrDate)

      val dfBlockedNumbers = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.SMS_OPT_OUT, DataSets.SOLUTIONS_INFINITI, DataSets.FULL_MERGE_MODE, incrDate)

      val dfZoneCity = DataReader.getDataFrame4mCsv(ConfigConstants.ZONE_CITY_PINCODE_PATH, "true", ";")

      val yestItr = CampaignInput.loadYesterdayItrSimpleData()

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
        dfZoneCity,
        yestItr)
    } else {
      readDf(incrDate, prevDate)
    }
  }

  /**
   *
   *
   * @param dfSalesOrderFull
   * @param dfSalesOrderItemCalcPrevFull
   * @param dfYestItr
   * @return
   */
  def getdfMostPreferredBrand(dfSalesOrderFull: DataFrame, dfSalesOrderItemCalcPrevFull: DataFrame, dfYestItr: DataFrame): DataFrame = {

    if (dfSalesOrderFull == null || dfSalesOrderItemCalcPrevFull == null || dfYestItr == null) {
      log("Data frame should not be null")
      return null
    }

    //join SalesOrder and SalesOrderItem Data
    val dfJoinOrderAndItem = dfSalesOrderFull.join(dfSalesOrderItemCalcPrevFull, dfSalesOrderItemCalcPrevFull(SalesOrderItemVariables.FK_SALES_ORDER) === dfSalesOrderFull(SalesOrderVariables.ID_SALES_ORDER), SQL.INNER)
      .select(SalesOrderVariables.FK_CUSTOMER, SalesOrderItemVariables.SKU)

    //join Itr and dfJoinOrderAndItem
    val dfJoinItrAndOrder = dfYestItr.join(dfJoinOrderAndItem, dfJoinOrderAndItem(SalesOrderItemVariables.SKU) === dfYestItr(ProductVariables.SKU_SIMPLE), SQL.INNER)
      .select(SalesOrderVariables.FK_CUSTOMER, ProductVariables.BRAND, ProductVariables.SPECIAL_PRICE)

    val dfGrouped = dfJoinItrAndOrder.groupBy(SalesOrderVariables.FK_CUSTOMER, ProductVariables.BRAND).agg(count(ProductVariables.BRAND) as COUNT_BRAND, sum(ProductVariables.SPECIAL_PRICE) as SUM_SPECIAL_PRICE)

    val dfResult = dfGrouped.sort(COUNT_BRAND, SUM_SPECIAL_PRICE).groupBy(SalesOrderVariables.FK_CUSTOMER).agg(last(ProductVariables.BRAND))

    dfResult
  }

}
