package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ ContactListMobileVars, _ }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.common.{ OptionUtils, Spark }
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.MergeUtils
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.customer.variables.CustomerSegments
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
object ContactListMobile extends Logging {

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
      dfContactListMobilePrevFull,
      dfCustomerSegmentsIncr,
      dfNLSIncr,
      dfSalesOrderIncr,
      dfSalesOrderFull,
      dfSalesOrderAddrFull,
      dfSalesOrderAddrFavPrevFull,
      dfSalesOrderItemIncr,
      dfSalesOrderCalcPrevFull,
      dfSuccessOrdersCalcPrevFull,
      dfFavBrandCalcPrevFull,
      dfYestItr,
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
    if (DataWriter.canWrite(saveMode, pathSalesOrderFavFull)) {
      DataWriter.writeParquet(dfSalesOrderAddrFavFull, pathSalesOrderFavFull, saveMode)
    }
    //call SalesOrder.processVariable for LAST_ORDER_DATE variable
    //TODO Check logic. Seems to be incorrect
    val dfSalesOrderCalcFull = SalesOrder.processVariables(dfSalesOrderCalcPrevFull, dfSalesOrderIncr).cache()
    //FK_CUSTOMER, LAST_ORDER_DATE, UPDATED_AT, FIRST_ORDER_DATE, ORDERS_COUNT, DAYS_SINCE_LAST_ORDER

    val pathSalesOrderCalcFull = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ORDER, DataSets.FULL_MERGE_MODE, incrDate)
    if (DataWriter.canWrite(saveMode, pathSalesOrderCalcFull)) {
      DataWriter.writeParquet(dfSalesOrderCalcFull, pathSalesOrderCalcFull, saveMode)
    }

    //SalesOrderItem.getSucessfulOrders for NET_ORDERS for variable
    val salesOrderFull = dfSalesOrderFull.select(SalesOrderVariables.ID_SALES_ORDER, SalesOrderVariables.FK_CUSTOMER)
    val (dfSuccessfulOrders, successOrdersCalcFull, dfFavBrandIncr, favBrandCalcFull) = SalesOrderItem.getSuccessfullOrdersBrand (
      dfSalesOrderItemIncr, salesOrderFull, dfSuccessOrdersCalcPrevFull, dfFavBrandCalcPrevFull, dfYestItr)
    //ORDERS_COUNT_SUCCESSFUL, FAV_BRAND

    val pathSuccessOrdersCalcFull = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SUCCESSFUL_ORDERS_COUNT, DataSets.FULL_MERGE_MODE, incrDate)
    if (DataWriter.canWrite(saveMode, pathSuccessOrdersCalcFull)) {
      DataWriter.writeParquet(successOrdersCalcFull, pathSuccessOrdersCalcFull, saveMode)
    }

    val pathFavBrandCalcFull = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.FAV_BRAND, DataSets.FULL_MERGE_MODE, incrDate)
    if (DataWriter.canWrite(saveMode, pathFavBrandCalcFull)) {
      DataWriter.writeParquet(favBrandCalcFull, pathFavBrandCalcFull, saveMode)
    }

    //Save Data Frame Contact List Mobile
    val (dfContactListMobileIncr, dfContactListMobileFull) = getContactListMobileDF (
      dfCustomerIncr,
      dfContactListMobilePrevFull,
      dfCustSegCalcIncr,
      dfNLSIncr,
      dfSalesOrderAddrFavCalc,
      dfSalesOrderCalcFull,
      dfSuccessfulOrders,
      dfFavBrandIncr,
      dfDND,
      dfSmsOptOut,
      dfBlockedNumbers,
      dfZoneCity)

    val pathContactListMobileFull = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CONTACT_LIST_MOBILE, DataSets.FULL_MERGE_MODE, incrDate)
    if (DataWriter.canWrite(saveMode, pathContactListMobileFull)) {
      DataWriter.writeParquet(dfContactListMobileFull, pathContactListMobileFull, saveMode)
    }

    val pathContactListMobile = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CONTACT_LIST_MOBILE, DataSets.DAILY_MODE, incrDate)
    if (DataWriter.canWrite(saveMode, pathContactListMobile)) {
      DataWriter.writeParquet(dfContactListMobileIncr, pathContactListMobile, saveMode)
    }

    val dfCsv = dfContactListMobileIncr.select(
      col(CustomerVariables.ID_CUSTOMER) as ContactListMobileVars.UID,
      col(CustomerVariables.EMAIL) as ContactListMobileVars.EMAIL,
      col(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS),
      col(CustomerVariables.PHONE) as ContactListMobileVars.MOBILE,
      col(ContactListMobileVars.MOBILE_PERMISION_STATUS),
      col(ContactListMobileVars.CITY),
      col(ContactListMobileVars.COUNTRY),
      col(ContactListMobileVars.FIRST_NAME),
      col(ContactListMobileVars.LAST_NAME),
      col(ContactListMobileVars.DOB),
      col(ContactListMobileVars.MVP_TYPE),
      col(ContactListMobileVars.NET_ORDERS),
      col(ContactListMobileVars.LAST_ORDER_DATE),
      col(ContactListMobileVars.GENDER),
      col(ContactListMobileVars.REG_DATE),
      col(ContactListMobileVars.SEGMENT),
      col(ContactListMobileVars.AGE),
      col(ContactListMobileVars.PLATINUM_STATUS),
      col(ContactListMobileVars.IS_REFERED),
      col(ContactListMobileVars.NL_SUB_DATE),
      col(ContactListMobileVars.VERIFICATION_STATUS),
      col(ContactListMobileVars.LAST_UPDATE_DATE),
      col(ContactListMobileVars.UNSUB_KEY),
      col(ContactListMobileVars.CITY_TIER),
      col(ContactListMobileVars.STATE_ZONE),
      col(ContactListMobileVars.DISCOUNT_SCORE),
      col(ContactListMobileVars.DND)
    )
    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    DataWriter.writeCsv(dfCsv, DataSets.VARIABLES, DataSets.CONTACT_LIST_MOBILE, DataSets.DAILY_MODE, incrDate, "53699_28334_" + fileDate + "_CONTACTS_LIST_MOBILE", DataSets.IGNORE_SAVEMODE, "true", ";")

  }

  /**
   *
   * @param dfCustomerIncr Bob's customer table data for the yesterday's date
   * @param dfContactListMobilePrevFull Day Before yestreday's data for contact List mobile file.
   * @param dfCustSegCalcIncr
   * @param dfNLSIncr
   * @param dfSalesOrderAddrFavCalc
   * @param dfSalesOrderCalcFull
   * @param dfSuccessfulOrders
   * @param dfZoneCity
   * @return
   */
  def getContactListMobileDF(
    dfCustomerIncr: DataFrame,
    dfContactListMobilePrevFull: DataFrame,
    dfCustSegCalcIncr: DataFrame,
    dfNLSIncr: DataFrame,
    dfSalesOrderAddrFavCalc: DataFrame,
    dfSalesOrderCalcFull: DataFrame,
    dfSuccessfulOrders: DataFrame,
    dfFavBrandIncr: DataFrame,
    dfDND: DataFrame,
    dfSmsOptOut: DataFrame,
    dfBlockedNumbers: DataFrame,
    dfZoneCity: DataFrame): (DataFrame, DataFrame) = {

    println("inside the getContactListMobileDF")

    if (null == dfCustomerIncr || null == dfNLSIncr || null == dfDND || null == dfSmsOptOut || null == dfBlockedNumbers || null == dfZoneCity) {
      log("Data frame should not be null")
      return null
    }

    println("After null check")

    val nls = dfNLSIncr.select(
      col(NewsletterVariables.EMAIL),
      col(NewsletterVariables.FK_CUSTOMER),
      col(NewsletterVariables.STATUS),
      col(NewsletterVariables.UNSUBSCRIBE_KEY),
      col(NewsletterVariables.CREATED_AT),
      col(NewsletterVariables.UPDATED_AT))

    println("after nls calc " + nls.count())

    val dfSmsOptOutMerged = dfSmsOptOut.select(DNDVariables.MOBILE_NUMBER).unionAll(dfBlockedNumbers.select(DNDVariables.MOBILE_NUMBER)).dropDuplicates()

    println("after smsOptOutMerged " + dfSmsOptOutMerged.count())

    val dfMergedIncr = mergeIncrData(dfCustomerIncr, dfCustSegCalcIncr, nls, dfSalesOrderAddrFavCalc, dfSalesOrderCalcFull, dfSuccessfulOrders, dfFavBrandIncr, dfZoneCity, dfDND, dfSmsOptOutMerged)

    println("after mergeIncrData " + dfMergedIncr.count())

    if (null != dfContactListMobilePrevFull) {

      println("inside dfContactListMobilePrevFull not null")

      //join old and new data frame
      val joinDF = MergeUtils.joinOldAndNewDF(dfMergedIncr, dfContactListMobilePrevFull, CustomerVariables.ID_CUSTOMER)

      //merge old and new data frame
      val dfFull = joinDF.select(
        coalesce(joinDF(CustomerVariables.NEW_ + CustomerVariables.ID_CUSTOMER), joinDF(CustomerVariables.ID_CUSTOMER)) as CustomerVariables.ID_CUSTOMER,

        Udf.latestString(joinDF(CustomerVariables.EMAIL), joinDF(CustomerVariables.NEW_ + CustomerVariables.EMAIL)) as CustomerVariables.EMAIL,

        Udf.latestString(joinDF(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS), joinDF(CustomerVariables.NEW_ + ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS)) as ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS,

        Udf.latestString(joinDF(CustomerVariables.PHONE), joinDF(CustomerVariables.NEW_ + CustomerVariables.PHONE)) as CustomerVariables.PHONE,

        coalesce(joinDF(CustomerVariables.NEW_ + ContactListMobileVars.MOBILE_PERMISION_STATUS), joinDF(ContactListMobileVars.MOBILE_PERMISION_STATUS)) as ContactListMobileVars.MOBILE_PERMISION_STATUS,

        Udf.latestString(joinDF(CustomerVariables.CITY), joinDF(CustomerVariables.NEW_ + CustomerVariables.CITY)) as CustomerVariables.CITY,

        lit("IN") as ContactListMobileVars.COUNTRY,

        Udf.latestString(joinDF(CustomerVariables.FIRST_NAME), joinDF(CustomerVariables.NEW_ + CustomerVariables.FIRST_NAME)) as CustomerVariables.FIRST_NAME,

        Udf.latestString(joinDF(CustomerVariables.LAST_NAME), joinDF(CustomerVariables.NEW_ + CustomerVariables.LAST_NAME)) as CustomerVariables.LAST_NAME,

        Udf.latestDate(joinDF(ContactListMobileVars.DOB), joinDF(CustomerVariables.NEW_ + ContactListMobileVars.DOB)) as ContactListMobileVars.DOB,

        coalesce(joinDF(CustomerVariables.NEW_ + ContactListMobileVars.MVP_TYPE), joinDF(ContactListMobileVars.MVP_TYPE)) as ContactListMobileVars.MVP_TYPE,

        joinDF(CustomerVariables.NEW_ + ContactListMobileVars.NET_ORDERS) + joinDF(ContactListMobileVars.NET_ORDERS) as ContactListMobileVars.NET_ORDERS,

        coalesce(joinDF(CustomerVariables.NEW_ + SalesOrderItemVariables.FAV_BRAND), joinDF(SalesOrderItemVariables.FAV_BRAND)) as SalesOrderItemVariables.FAV_BRAND,

        coalesce(joinDF(CustomerVariables.NEW_ + ContactListMobileVars.LAST_ORDER_DATE), joinDF(ContactListMobileVars.LAST_ORDER_DATE)) as ContactListMobileVars.LAST_ORDER_DATE,

        Udf.latestString(joinDF(CustomerVariables.GENDER), joinDF(CustomerVariables.NEW_ + CustomerVariables.GENDER)) as CustomerVariables.GENDER,

        Udf.minTimestamp(joinDF(ContactListMobileVars.REG_DATE), joinDF(CustomerVariables.NEW_ + ContactListMobileVars.REG_DATE)) as ContactListMobileVars.REG_DATE,

        coalesce(joinDF(CustomerVariables.NEW_ + CustomerSegmentsVariables.SEGMENT), joinDF(CustomerSegmentsVariables.SEGMENT)) as CustomerSegmentsVariables.SEGMENT,

        coalesce(joinDF(CustomerVariables.NEW_ + ContactListMobileVars.AGE), joinDF(ContactListMobileVars.AGE)) as ContactListMobileVars.AGE,

        Udf.latestString(joinDF(ContactListMobileVars.PLATINUM_STATUS), joinDF(CustomerVariables.NEW_ + ContactListMobileVars.PLATINUM_STATUS)) as ContactListMobileVars.PLATINUM_STATUS,

        lit("") as ContactListMobileVars.IS_REFERED, //IS_REFERRED

        Udf.latestTimestamp(joinDF(ContactListMobileVars.NL_SUB_DATE), joinDF(CustomerVariables.NEW_ + ContactListMobileVars.NL_SUB_DATE)) as ContactListMobileVars.NL_SUB_DATE,

        Udf.latestBool(joinDF(ContactListMobileVars.VERIFICATION_STATUS), joinDF(CustomerVariables.NEW_ + ContactListMobileVars.VERIFICATION_STATUS)) as ContactListMobileVars.VERIFICATION_STATUS,

        Udf.maxTimestamp(joinDF(CustomerVariables.LAST_UPDATED_AT), joinDF(CustomerVariables.NEW_ + CustomerVariables.LAST_UPDATED_AT)) as CustomerVariables.LAST_UPDATED_AT,

        Udf.latestString(joinDF(ContactListMobileVars.UNSUB_KEY), joinDF(CustomerVariables.NEW_ + ContactListMobileVars.UNSUB_KEY)) as ContactListMobileVars.UNSUB_KEY,

        coalesce(joinDF(CustomerVariables.NEW_ + ContactListMobileVars.CITY_TIER), joinDF(ContactListMobileVars.CITY_TIER)) as ContactListMobileVars.CITY_TIER,

        coalesce(joinDF(CustomerVariables.NEW_ + ContactListMobileVars.STATE_ZONE), joinDF(ContactListMobileVars.STATE_ZONE)) as ContactListMobileVars.STATE_ZONE,

        coalesce(joinDF(CustomerVariables.NEW_ + CustomerSegmentsVariables.DISCOUNT_SCORE), joinDF(CustomerSegmentsVariables.DISCOUNT_SCORE)) as CustomerSegmentsVariables.DISCOUNT_SCORE,

        coalesce(joinDF(CustomerVariables.NEW_ + ContactListMobileVars.DND), joinDF(ContactListMobileVars.DND)) as ContactListMobileVars.DND // DND
      )
      (dfFull.except(dfContactListMobilePrevFull), dfFull)
    } else {
      println("With dfContactListMobilePrevFull null")
      (dfMergedIncr, dfMergedIncr)
    }

  }

  def mergeIncrData(customerIncr: DataFrame, custSegCalcIncr: DataFrame, nls: DataFrame, salesAddrCalFull: DataFrame, salesOrderCalcFull: DataFrame, successfulOrdersIncr: DataFrame, favBrandIncr: DataFrame, cityZone: DataFrame, dnd: DataFrame, smsOptOut: DataFrame): DataFrame = {
    println("Inside mergeIncrData")

    val customerSeg = customerIncr.join(custSegCalcIncr, customerIncr(CustomerVariables.ID_CUSTOMER) === custSegCalcIncr(CustomerSegmentsVariables.FK_CUSTOMER), SQL.FULL_OUTER)
      .select(
        coalesce(customerIncr(CustomerVariables.ID_CUSTOMER), custSegCalcIncr(CustomerSegmentsVariables.FK_CUSTOMER)) as CustomerVariables.ID_CUSTOMER,
        customerIncr(CustomerVariables.EMAIL),
        customerIncr(CustomerVariables.BIRTHDAY) as ContactListMobileVars.DOB,
        customerIncr(CustomerVariables.GENDER),
        customerIncr(CustomerVariables.CREATED_AT),
        customerIncr(CustomerVariables.FIRST_NAME),
        customerIncr(CustomerVariables.LAST_NAME),
        customerIncr(CustomerVariables.PHONE),
        customerIncr(CustomerVariables.IS_CONFIRMED) as ContactListMobileVars.VERIFICATION_STATUS,
        Udf.age(customerIncr(CustomerVariables.BIRTHDAY)) as ContactListMobileVars.AGE,
        customerIncr(CustomerVariables.REWARD_TYPE) as ContactListMobileVars.PLATINUM_STATUS,
        customerIncr(CustomerVariables.UPDATED_AT),
        custSegCalcIncr(ContactListMobileVars.MVP_TYPE),
        custSegCalcIncr(CustomerSegmentsVariables.SEGMENT),
        custSegCalcIncr(CustomerSegmentsVariables.DISCOUNT_SCORE))

    println("After custSegCalcIncr and customerIncr merge ")

    val customerMerged = customerSeg.join(nls, nls(NewsletterVariables.EMAIL) === customerSeg(CustomerVariables.EMAIL), SQL.FULL_OUTER)
      .select(
        coalesce(customerSeg(CustomerVariables.ID_CUSTOMER), nls(NewsletterVariables.FK_CUSTOMER)) as CustomerVariables.ID_CUSTOMER,
        customerSeg(CustomerVariables.EMAIL),
        customerSeg(ContactListMobileVars.DOB),
        customerSeg(CustomerVariables.GENDER),
        Udf.minTimestamp(customerSeg(CustomerVariables.CREATED_AT), nls(NewsletterVariables.CREATED_AT)) as ContactListMobileVars.REG_DATE,
        customerSeg(ContactListMobileVars.VERIFICATION_STATUS),
        customerSeg(ContactListMobileVars.AGE),
        customerSeg(CustomerVariables.FIRST_NAME),
        customerSeg(CustomerVariables.LAST_NAME),
        customerSeg(CustomerVariables.PHONE),
        customerSeg(ContactListMobileVars.PLATINUM_STATUS),
        customerSeg(ContactListMobileVars.MVP_TYPE),
        customerSeg(CustomerSegmentsVariables.SEGMENT),
        customerSeg(CustomerSegmentsVariables.DISCOUNT_SCORE),
        Udf.udfEmailOptInStatus(nls(NewsletterVariables.EMAIL), nls(NewsletterVariables.STATUS)) as ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS,
        nls(NewsletterVariables.CREATED_AT) as ContactListMobileVars.NL_SUB_DATE,
        nls(NewsletterVariables.UNSUBSCRIBE_KEY) as ContactListMobileVars.UNSUB_KEY,
        Udf.maxTimestamp(customerSeg(CustomerVariables.UPDATED_AT), nls(NewsletterVariables.UPDATED_AT)) as CustomerVariables.UPDATED_AT)

    println("After custSegCalcIncr, customerIncr, nls merge ")

    val salesOrderAddress = salesAddrCalFull.join(salesOrderCalcFull, salesAddrCalFull(SalesOrderVariables.FK_CUSTOMER) === salesOrderCalcFull(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
      .select(
        coalesce(salesAddrCalFull(SalesOrderVariables.FK_CUSTOMER), salesOrderCalcFull(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
        salesAddrCalFull(SalesAddressVariables.CITY),
        salesAddrCalFull(SalesAddressVariables.PHONE),
        salesAddrCalFull(SalesAddressVariables.FIRST_NAME),
        salesAddrCalFull(SalesAddressVariables.LAST_NAME),
        salesOrderCalcFull(ContactListMobileVars.LAST_ORDER_DATE),
        salesOrderCalcFull(SalesOrderVariables.UPDATED_AT))

    println("After salesAddrCalFull, salesOrderCalcFull merge ")

    val salesMerged = salesOrderAddress.join(successfulOrdersIncr, successfulOrdersIncr(SalesOrderVariables.FK_CUSTOMER) === salesOrderAddress(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
      .select(
        coalesce(salesOrderAddress(SalesOrderVariables.FK_CUSTOMER), successfulOrdersIncr(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
        salesOrderAddress(SalesAddressVariables.CITY),
        salesOrderAddress(SalesAddressVariables.PHONE),
        salesOrderAddress(SalesAddressVariables.FIRST_NAME),
        salesOrderAddress(SalesAddressVariables.LAST_NAME),
        salesOrderAddress(ContactListMobileVars.LAST_ORDER_DATE),
        salesOrderAddress(SalesOrderVariables.UPDATED_AT),
        successfulOrdersIncr(SalesOrderItemVariables.ORDERS_COUNT_SUCCESSFUL))

    println("After salesAddrCalFull, salesOrderCalcFull, successfulOrdersIncr merge ")


    val brandMerged = salesMerged.join(favBrandIncr, salesMerged(SalesOrderVariables.FK_CUSTOMER) === favBrandIncr(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
      .select(
        coalesce(salesMerged(SalesOrderVariables.FK_CUSTOMER), favBrandIncr(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
        salesMerged(SalesAddressVariables.CITY),
        salesMerged(SalesAddressVariables.PHONE),
        salesMerged(SalesAddressVariables.FIRST_NAME),
        salesMerged(SalesAddressVariables.LAST_NAME),
        salesMerged(ContactListMobileVars.LAST_ORDER_DATE),
        salesMerged(SalesOrderVariables.UPDATED_AT),
        salesMerged(SalesOrderItemVariables.ORDERS_COUNT_SUCCESSFUL),
        favBrandIncr(SalesOrderItemVariables.FAV_BRAND))

    println("After salesAddrCalFull, salesOrderCalcFull, successfulOrdersIncr, favBrandIncr merge ")

    val mergedIncr = customerMerged.join(brandMerged, brandMerged(SalesOrderVariables.FK_CUSTOMER) === customerMerged(CustomerVariables.ID_CUSTOMER))
      .select(
        coalesce(customerMerged(CustomerVariables.ID_CUSTOMER), brandMerged(SalesOrderVariables.FK_CUSTOMER)) as CustomerVariables.ID_CUSTOMER,
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
        brandMerged(SalesAddressVariables.CITY),
        coalesce(customerMerged(CustomerVariables.FIRST_NAME), brandMerged(SalesAddressVariables.FIRST_NAME)) as CustomerVariables.FIRST_NAME,
        coalesce(customerMerged(CustomerVariables.LAST_NAME), brandMerged(SalesAddressVariables.LAST_NAME)) as CustomerVariables.LAST_NAME,
        coalesce(customerMerged(CustomerVariables.PHONE), brandMerged(SalesAddressVariables.PHONE)) as SalesAddressVariables.PHONE,
        brandMerged(ContactListMobileVars.LAST_ORDER_DATE),
        Udf.maxTimestamp(brandMerged(SalesOrderVariables.UPDATED_AT), customerMerged(CustomerVariables.UPDATED_AT)) as CustomerVariables.LAST_UPDATED_AT,
        brandMerged(SalesOrderItemVariables.ORDERS_COUNT_SUCCESSFUL) as ContactListMobileVars.NET_ORDERS,
        brandMerged(SalesOrderItemVariables.FAV_BRAND))

    println("After custSegCalcIncr, customerIncr, nls, salesAddrCalFull, salesOrderCalcFull, successfulOrdersIncr, favBrandIncr merge ")

    val cityBc = Spark.getContext().broadcast(cityZone).value
    cityZone.printSchema()
    println(cityZone.count())

    val cityJoined = mergedIncr.join(cityBc, Udf.toLowercase(cityBc(ContactListMobileVars.CITY)) === Udf.toLowercase(mergedIncr(SalesAddressVariables.CITY)), SQL.LEFT_OUTER)
      .select(
        mergedIncr(CustomerVariables.ID_CUSTOMER),
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
        mergedIncr(SalesAddressVariables.PHONE),
        mergedIncr(ContactListMobileVars.LAST_ORDER_DATE),
        mergedIncr(CustomerVariables.LAST_UPDATED_AT),
        mergedIncr(ContactListMobileVars.NET_ORDERS),
        mergedIncr(SalesOrderItemVariables.FAV_BRAND),
        cityBc(CustomerVariables.ZONE) as ContactListMobileVars.STATE_ZONE,
        cityBc(CustomerVariables.TIER1) as ContactListMobileVars.CITY_TIER)

    println("After custSegCalcIncr, customerIncr, nls, salesAddrCalFull, salesOrderCalcFull, successfulOrdersIncr, favBrandIncr, cityZone merge ")

    val dndBc = Spark.getContext().broadcast(dnd).value

    val dndMerged = cityJoined.join(dndBc, dndBc(DNDVariables.MOBILE_NUMBER) === cityJoined(SalesAddressVariables.PHONE), SQL.LEFT_OUTER)
      .select(
        cityJoined(CustomerVariables.ID_CUSTOMER),
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
        cityJoined(SalesAddressVariables.PHONE),
        cityJoined(ContactListMobileVars.LAST_ORDER_DATE),
        cityJoined(CustomerVariables.LAST_UPDATED_AT),
        cityJoined(ContactListMobileVars.NET_ORDERS),
        cityJoined(SalesOrderItemVariables.FAV_BRAND),
        cityJoined(ContactListMobileVars.STATE_ZONE),
        cityJoined(ContactListMobileVars.CITY_TIER),
        when(dndBc(DNDVariables.MOBILE_NUMBER).!==(null), "1").otherwise("0") as ContactListMobileVars.DND)

    println("After custSegCalcIncr, customerIncr, nls, salesAddrCalFull, salesOrderCalcFull, successfulOrdersIncr, favBrandIncr, cityZone, dnd merge ")

    val smsBc = Spark.getContext().broadcast(smsOptOut).value

    val res = dndMerged.join(smsBc, dndMerged(SalesAddressVariables.PHONE) === smsBc(DNDVariables.MOBILE_NUMBER), SQL.LEFT_OUTER)
      .select(
        dndMerged(CustomerVariables.ID_CUSTOMER),
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
        dndMerged(SalesAddressVariables.PHONE),
        dndMerged(ContactListMobileVars.LAST_ORDER_DATE),
        dndMerged(CustomerVariables.LAST_UPDATED_AT),
        dndMerged(ContactListMobileVars.NET_ORDERS),
        dndMerged(SalesOrderItemVariables.FAV_BRAND),
        dndMerged(ContactListMobileVars.STATE_ZONE),
        dndMerged(ContactListMobileVars.CITY_TIER),
        dndMerged(ContactListMobileVars.DND),
        when(smsBc(DNDVariables.MOBILE_NUMBER).!==(null), "o").otherwise("i") as ContactListMobileVars.MOBILE_PERMISION_STATUS
      )

    println("After custSegCalcIncr, customerIncr, nls, salesAddrCalFull, salesOrderCalcFull, successfulOrdersIncr, favBrandIncr, cityZone, dnd, smsOptOut merge")

    res
  }

  /**
   * read Data Frames
   * @param incrDate
   * @return
   */
  def readDf(incrDate: String, prevDate: String): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {

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

    val dfSuccessOrdersCalcPrevFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SUCCESSFUL_ORDERS_COUNT, DataSets.FULL_MERGE_MODE, prevDate)

    val dfFavBrandCalcPrevFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.FAV_BRAND, DataSets.FULL_MERGE_MODE, prevDate)

    val dfYestItr = CampaignInput.loadYesterdayItrSimpleData()

    val dfDND = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.RESPONSYS, DataSets.DND, DataSets.FULL_MERGE_MODE, incrDate)

    val dfSmsOptOut = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.RESPONSYS, DataSets.SMS_OPT_OUT, DataSets.FULL_MERGE_MODE, incrDate)

    val dfBlockedNumbers = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.SOLUTIONS_INFINITI, DataSets.BLOCK_LIST_NUMBERS, DataSets.FULL_MERGE_MODE, incrDate)

    val dfZoneCity = DataReader.getDataFrame4mCsv(ConfigConstants.ZONE_CITY_PINCODE_PATH, "true", ",")
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
      dfSuccessOrdersCalcPrevFull,
      dfFavBrandCalcPrevFull,
      dfYestItr,
      dfDND,
      dfSmsOptOut,
      dfBlockedNumbers,
      dfZoneCity)
  }

  def readDf(paths: String, incrDate: String, prevDate: String): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {
    if (null != paths) {
      val dfCustomerIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CUSTOMER, DataSets.FULL_MERGE_MODE, incrDate)
      val dfCustomerSegmentsIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CUSTOMER_SEGMENTS, DataSets.FULL_MERGE_MODE, incrDate)
      val dfNLSIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.NEWSLETTER_SUBSCRIPTION, DataSets.FULL_MERGE_MODE, incrDate)
      val dfSalesOrderFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.FULL_MERGE_MODE, incrDate)
      val dfSalesOrderIncr = dfSalesOrderFull
      val dfSalesOrderAddrFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ADDRESS, DataSets.FULL_MERGE_MODE, incrDate)

      val dfSalesOrderItemIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ITEM, DataSets.FULL_MERGE_MODE, incrDate)

      val dfYestItr = CampaignInput.loadYesterdayItrSimpleData(incrDate)

      val dfDND = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.RESPONSYS, DataSets.DND, DataSets.FULL_MERGE_MODE, incrDate)

      val dfSmsOptOut = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.RESPONSYS, DataSets.SMS_OPT_OUT, DataSets.FULL_MERGE_MODE, incrDate)

      val dfBlockedNumbers = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.SOLUTIONS_INFINITI, DataSets.BLOCK_LIST_NUMBERS, DataSets.FULL_MERGE_MODE, incrDate)

      val dfZoneCity = DataReader.getDataFrame4mCsv(ConfigConstants.ZONE_CITY_PINCODE_PATH, "true", ",")

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
        null,
        dfYestItr,
        dfDND,
        dfSmsOptOut,
        dfBlockedNumbers,
        dfZoneCity)
    } else {
      readDf(incrDate, prevDate)
    }
  }

}