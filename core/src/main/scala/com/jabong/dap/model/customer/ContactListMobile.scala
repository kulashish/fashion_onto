package com.jabong.dap.model.customer

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.common.schema.SchemaUtils
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.MergeUtils
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.customer.variables.{Customer, CustomerSegments}
import com.jabong.dap.model.order.variables.{SalesOrder, SalesOrderAddress, SalesOrderItem}
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * This File generates the conatct_list_mobile.csv for email campaigns.
 * Created by raghu on 17/8/15.
 */
object ContactListMobile extends Logging {

  /**
   * Start Method for the conatct_list_mobile.csv generation for email campaigns.
   * @param vars Input parameters like for which date to do and saveMode, Etc.
   */
  def start(vars: ParamInfo) = {

    val incrDate = OptionUtils.getOptValue(vars.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val saveMode = vars.saveMode

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
      dfSalesOrderCalculatedPrevFull,
      dfDND,
      dfZoneCity
    ) = readDf(incrDate)

    //get  Customer CustomerSegments.getCustomerSegments
    val dfCustomerSegmentsInc = CustomerSegments.getCustomerSegments(dfCustomerSegmentsIncr)

    //call SalesOrderAddress.processVariable
    val (dfSalesOrderAddrFavCalculated, dfSalesOrderAddrFavFull) = SalesOrderAddress.processVariable(dfSalesOrderIncr, dfSalesOrderAddrFull, dfSalesOrderAddrFavPrevFull)
    val pathSalesOrderFavFull = DataWriter.getWritePath(ConfigConstants.OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ORDER_ADDRESS, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(dfSalesOrderAddrFavFull, pathSalesOrderFavFull, saveMode)

    //call SalesOrder.processVariable for LAST_ORDER_DATE variable
    val dfSalesOrderCalculatedFull = SalesOrder.processVariables(dfSalesOrderCalculatedPrevFull, dfSalesOrderIncr)
    val pathSalesOrderCalculatedFull = DataWriter.getWritePath(ConfigConstants.OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ORDER, DataSets.DAILY_MODE, incrDate)
    DataWriter.writeParquet(dfSalesOrderCalculatedFull, pathSalesOrderCalculatedFull, saveMode)

    //SalesOrderItem.getSucessfulOrders for NET_ORDERS for variable
    val dfSuccessfulOrders = SalesOrderItem.getSucessfullOrders(dfSalesOrderFull, dfSalesOrderItemIncr)

    //Save Data Frame Contact List Mobile
    val (dfContactListMobileInc, dfContactListMobileFull) = getContactListMobileDF (
      dfCustomerIncr,
      dfCustomerListMobilePrevFull,
      dfCustomerSegmentsInc,
      dfNLSIncr,
      dfSalesOrderAddrFavCalculated,
      dfSalesOrderCalculatedFull,
      dfSuccessfulOrders,
      dfDND,
      dfZoneCity)

    val pathContactListMobileFull = DataWriter.getWritePath(ConfigConstants.OUTPUT_PATH, DataSets.VARIABLES, DataSets.CONTACT_LIST_MOBILE, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(dfContactListMobileFull, pathContactListMobileFull, saveMode)


    val pathContactListMobile = DataWriter.getWritePath(ConfigConstants.OUTPUT_PATH, DataSets.VARIABLES, DataSets.CONTACT_LIST_MOBILE, DataSets.DAILY_MODE, incrDate)
    DataWriter.writeParquet(dfContactListMobileInc, pathContactListMobile, saveMode)

  }

  /**
   *
   * @param dfCustomerIncr Bob's customer table data for the yesterday's date
   * @param dfCustomerListMobilePrevFull Day Before yestreday's data for contact List mobile file.
   * @param dfCustomerSegmentsIncr
   * @param dfNLSIncr
   * @param dfSalesOrderAddressIncr
   * @param dfSalesOrder
   * @param dfSuccessfulOrders
   * @param dfZoneCity
   * @return
   */
  def getContactListMobileDF(
                             dfCustomerIncr: DataFrame,
                             dfCustomerListMobilePrevFull: DataFrame,
                             dfCustomerSegmentsIncr: DataFrame,
                             dfNLSIncr: DataFrame,
                             dfSalesOrderAddressIncr: DataFrame,
                             dfSalesOrder: DataFrame,
                             dfSuccessfulOrders: DataFrame,
                             dfDND: DataFrame,
                             dfZoneCity: DataFrame): (DataFrame, DataFrame) = {

    if (dfCustomerIncr == null || dfCustomerSegmentsIncr == null || dfNLSIncr == null || dfSalesOrderAddressIncr == null) {

      log("Data frame should not be null")

      return null

    }

    if (!SchemaUtils.isSchemaEqual(dfCustomerIncr.schema, Schema.customer) ||
      !SchemaUtils.isSchemaEqual(dfCustomerSegmentsIncr.schema, Schema.customerSegments) ||
      !SchemaUtils.isSchemaEqual(dfNLSIncr.schema, Schema.nls) ||
      !SchemaUtils.isSchemaEqual(dfSalesOrderAddressIncr.schema, Schema.salesOrder)) {

      log("schema attributes or data type mismatch")

      return null

    }

    val NLS = dfNLSIncr.select(
      col(NewsletterVariables.EMAIL) as NewsletterVariables.NLS_EMAIL,
      col(NewsletterVariables.STATUS),
      col(NewsletterVariables.UNSUBSCRIBE_KEY),
      col(NewsletterVariables.CREATED_AT) as NewsletterVariables.NLS_CREATED_AT,
      col(NewsletterVariables.UPDATED_AT) as NewsletterVariables.NLS_UPDATED_AT
    )

    //Name of variable: CUSTOMERS PREFERRED ORDER TIMESLOT
    val udfCPOT = Customer.getCPOT(dfSalesOrderAddressIncr: DataFrame)

    val dfJoin = dfCustomerIncr.join(udfCPOT, dfCustomerIncr(CustomerVariables.ID_CUSTOMER) === udfCPOT(CustomerVariables.FK_CUSTOMER_CPOT), SQL.FULL_OUTER)

      .join(dfCustomerSegmentsIncr, dfCustomerIncr(CustomerVariables.ID_CUSTOMER) === dfCustomerSegmentsIncr(CustomerSegmentsVariables.FK_CUSTOMER), SQL.FULL_OUTER)

      .join(NLS, dfCustomerIncr(CustomerVariables.EMAIL) === NLS(NewsletterVariables.NLS_EMAIL), SQL.FULL_OUTER)

      .join(dfSalesOrderAddressIncr, dfCustomerIncr(CustomerVariables.ID_CUSTOMER) === dfSalesOrderAddressIncr(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)

      .join(dfSalesOrder, dfCustomerIncr(CustomerVariables.ID_CUSTOMER) === dfSalesOrder(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)

      .join(dfSuccessfulOrders, dfCustomerIncr(CustomerVariables.ID_CUSTOMER) === dfSuccessfulOrders(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)

    //Name of variable: EMAIL_SUBSCRIPTION_STATUS
    val udfEmailOptInStatus = udf((nls_email: String, status: String) => Customer.getEmailOptInStatus(nls_email: String, status: String))

    val dfInc = dfJoin.select(
      col(CustomerVariables.ID_CUSTOMER),
      col(CustomerVariables.GIFTCARD_CREDITS_AVAILABLE),
      col(CustomerVariables.STORE_CREDITS_AVAILABLE),
      col(CustomerVariables.BIRTHDAY) as CustomerVariables.DOB,
      Udf.age(dfJoin(CustomerVariables.BIRTHDAY)) as CustomerVariables.AGE,
      col(CustomerVariables.GENDER),
      col(CustomerVariables.REWARD_TYPE) as CustomerVariables.PLATINUM_STATUS,
      col(CustomerVariables.EMAIL),
      col(CustomerVariables.CREATED_AT),
      col(CustomerVariables.UPDATED_AT),
      col(CustomerVariables.CUSTOMER_ALL_ORDER_TIMESLOT),
      col(CustomerVariables.CUSTOMER_PREFERRED_ORDER_TIMESLOT),
      col(CustomerVariables.IS_CONFIRMED) as CustomerVariables.VERIFICATION_STATUS,

      col(CustomerSegmentsVariables.MVP_TYPE),
      col(CustomerSegmentsVariables.SEGMENT),
      col(CustomerSegmentsVariables.DISCOUNT_SCORE),

      col(NewsletterVariables.NLS_CREATED_AT) as NewsletterVariables.NL_SUB_DATE,
      col(NewsletterVariables.UNSUBSCRIBE_KEY) as NewsletterVariables.UNSUB_KEY,
      Udf.minTimestamp(dfJoin(CustomerVariables.CREATED_AT), dfJoin(NewsletterVariables.NLS_CREATED_AT)) as CustomerVariables.REG_DATE,
      Udf.maxTimestamp(dfJoin(CustomerVariables.UPDATED_AT), Udf.maxTimestamp(dfJoin(NewsletterVariables.NLS_UPDATED_AT), dfJoin(SalesOrderVariables.SO_UPDATED_AT))) as CustomerVariables.LAST_UPDATED_AT,
      udfEmailOptInStatus(dfJoin(NewsletterVariables.NLS_EMAIL), dfJoin(NewsletterVariables.STATUS)) as CustomerVariables.EMAIL_SUBSCRIPTION_STATUS,

      coalesce(col(CustomerVariables.FIRST_NAME), col(SalesAddressVariables.SOA_FIRST_NAME)) as CustomerVariables.FIRST_NAME,
      coalesce(col(CustomerVariables.LAST_NAME), col(SalesAddressVariables.SOA_LAST_NAME)) as CustomerVariables.LAST_NAME,
      coalesce(col(CustomerVariables.PHONE), col(SalesAddressVariables.MOBILE)) as SalesAddressVariables.MOBILE,
      col(SalesAddressVariables.CITY),
      lit("IN") as SalesAddressVariables.COUNTRY,
      col(SalesOrderVariables.LAST_ORDER_DATE),
      col(SalesOrderItemVariables.ORDERS_COUNT_SUCCESSFUL) as SalesOrderItemVariables.NET_ORDERS
    )

    var dfFull: DataFrame = dfInc

    if (null != dfCustomerListMobilePrevFull) {

      //join old and new data frame
      val joinDF = MergeUtils.joinOldAndNewDF(dfInc, dfCustomerListMobilePrevFull, CustomerVariables.ID_CUSTOMER)

      //merge old and new data frame
      dfFull = joinDF.select(
        Udf.latestInt(joinDF(CustomerVariables.ID_CUSTOMER), joinDF(CustomerVariables.NEW_ + CustomerVariables.ID_CUSTOMER)) as CustomerVariables.ID_CUSTOMER,

        Udf.latestDecimal(joinDF(CustomerVariables.GIFTCARD_CREDITS_AVAILABLE), joinDF(CustomerVariables.NEW_ + CustomerVariables.GIFTCARD_CREDITS_AVAILABLE)) as CustomerVariables.GIFTCARD_CREDITS_AVAILABLE,

        Udf.latestDecimal(joinDF(CustomerVariables.STORE_CREDITS_AVAILABLE), joinDF(CustomerVariables.NEW_ + CustomerVariables.STORE_CREDITS_AVAILABLE)) as CustomerVariables.STORE_CREDITS_AVAILABLE,

        Udf.latestDate(joinDF(CustomerVariables.BIRTHDAY), joinDF(CustomerVariables.NEW_ + CustomerVariables.BIRTHDAY)) as CustomerVariables.BIRTHDAY,

        Udf.latestString(joinDF(CustomerVariables.GENDER), joinDF(CustomerVariables.NEW_ + CustomerVariables.GENDER)) as CustomerVariables.GENDER,

        Udf.latestString(joinDF(CustomerVariables.REWARD_TYPE), joinDF(CustomerVariables.NEW_ + CustomerVariables.REWARD_TYPE)) as CustomerVariables.REWARD_TYPE,

        Udf.latestString(joinDF(CustomerVariables.EMAIL), joinDF(CustomerVariables.NEW_ + CustomerVariables.EMAIL)) as CustomerVariables.EMAIL,

        Udf.latestTimestamp(joinDF(CustomerVariables.CREATED_AT), joinDF(CustomerVariables.NEW_ + CustomerVariables.CREATED_AT)) as CustomerVariables.CREATED_AT,

        Udf.latestTimestamp(joinDF(CustomerVariables.UPDATED_AT), joinDF(CustomerVariables.NEW_ + CustomerVariables.UPDATED_AT)) as CustomerVariables.UPDATED_AT,

        Udf.mergeSlots(joinDF(CustomerVariables.CUSTOMER_ALL_ORDER_TIMESLOT), joinDF(CustomerVariables.NEW_ + CustomerVariables.CUSTOMER_ALL_ORDER_TIMESLOT)) as CustomerVariables.CUSTOMER_ALL_ORDER_TIMESLOT,

        Udf.maxSlot(joinDF(CustomerVariables.CUSTOMER_ALL_ORDER_TIMESLOT), joinDF(CustomerVariables.NEW_ + CustomerVariables.CUSTOMER_ALL_ORDER_TIMESLOT), joinDF(CustomerVariables.CUSTOMER_PREFERRED_ORDER_TIMESLOT)) as CustomerVariables.CUSTOMER_PREFERRED_ORDER_TIMESLOT,

        Udf.latestString(joinDF(CustomerVariables.FIRST_NAME), joinDF(CustomerVariables.NEW_ + CustomerVariables.FIRST_NAME)) as CustomerVariables.FIRST_NAME,

        Udf.latestString(joinDF(CustomerVariables.LAST_NAME), joinDF(CustomerVariables.NEW_ + CustomerVariables.LAST_NAME)) as CustomerVariables.LAST_NAME,

        Udf.latestString(joinDF(CustomerVariables.PHONE), joinDF(CustomerVariables.NEW_ + CustomerVariables.PHONE)) as CustomerVariables.PHONE,

        Udf.latestString(joinDF(CustomerVariables.CITY), joinDF(CustomerVariables.NEW_ + CustomerVariables.CITY)) as CustomerVariables.CITY,

        Udf.latestBool(joinDF(CustomerVariables.VERIFICATION_STATUS), joinDF(CustomerVariables.NEW_ + CustomerVariables.VERIFICATION_STATUS)) as CustomerVariables.VERIFICATION_STATUS,

        Udf.latestTimestamp(joinDF(NewsletterVariables.NL_SUB_DATE), joinDF(CustomerVariables.NEW_ + NewsletterVariables.NL_SUB_DATE)) as NewsletterVariables.NL_SUB_DATE,

        Udf.latestString(joinDF(NewsletterVariables.UNSUB_KEY), joinDF(CustomerVariables.NEW_ + NewsletterVariables.UNSUB_KEY)) as NewsletterVariables.UNSUB_KEY,

        Udf.latestInt(joinDF(CustomerVariables.AGE), joinDF(CustomerVariables.NEW_ + CustomerVariables.AGE)) as CustomerVariables.AGE,

        Udf.minTimestamp(joinDF(CustomerVariables.REG_DATE), joinDF(CustomerVariables.NEW_ + CustomerVariables.REG_DATE)) as CustomerVariables.REG_DATE,

        Udf.maxTimestamp(joinDF(CustomerVariables.LAST_UPDATED_AT), joinDF(CustomerVariables.NEW_ + CustomerVariables.LAST_UPDATED_AT)) as CustomerVariables.LAST_UPDATED_AT,

        Udf.latestString(joinDF(CustomerVariables.EMAIL_SUBSCRIPTION_STATUS), joinDF(CustomerVariables.NEW_ + CustomerVariables.EMAIL_SUBSCRIPTION_STATUS)) as CustomerVariables.EMAIL_SUBSCRIPTION_STATUS
      )
    }

    (dfInc, dfFull)
  }

  /**
   * read Data Frames
   * @param incrDate
   * @return
   */
//  dfCustomerIncr,
//  dfContactListMobilePrevFull,
//  dfCustomerSegmentsIncr,
//  dfNLSIncr,
//  dfSalesOrderIncr,
//  dfSalesOrderFull,
//  dfSalesOrderAddrFull,
//  dfSalesOrderAddrFavPrevFull,
//  dfSalesOrderItemIncr,
//  dfSalesOrderCalculatedPrevFull,
//  dfDND,
//  dfZoneCity
  def readDf(incrDate: String): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {
    val prevDate = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER, incrDate)

    val dfCustomerIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CUSTOMER, DataSets.DAILY_MODE, incrDate)
    val dfCustomerListMobilePrevFull = DataReader.getDataFrame(ConfigConstants.OUTPUT_PATH, DataSets.VARIABLES, DataSets.CONTACT_LIST_MOBILE, DataSets.FULL_MERGE_MODE, prevDate)

    val dfCustomerSegmentsIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CUSTOMER_SEGMENTS, DataSets.DAILY_MODE, incrDate)
    val dfNLSIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.NEWSLETTER_SUBSCRIPTION, DataSets.DAILY_MODE, incrDate)
    val dfSalesOrderIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.DAILY_MODE, incrDate)
    val dfSalesOrderFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.FULL_MERGE_MODE, incrDate)
    val dfSalesOrderAddrFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ADDRESS, DataSets.DAILY_MODE, incrDate)

    val dfSalesOrderAddrFavPrevFull = DataReader.getDataFrame(ConfigConstants.OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ORDER_ADDRESS, DataSets.FULL_MERGE_MODE, prevDate)

    val dfSalesOrderItemIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ITEM, DataSets.DAILY_MODE, incrDate)

    val dfSalesOrderCalculatedPrevFull = DataReader.getDataFrame(ConfigConstants.OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ORDER, DataSets.FULL_MERGE_MODE, prevDate)

    val dfDND = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.DCF, DataSets.DND, DataSets.DAILY_MODE, incrDate)
    val dfZoneCity = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.DCF, DataSets.ZONE_CITY, DataSets.DAILY_MODE, incrDate)

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
      dfSalesOrderCalculatedPrevFull,
      dfDND,
      dfZoneCity
    )
  }

}
