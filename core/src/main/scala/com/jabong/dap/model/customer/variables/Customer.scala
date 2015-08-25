package com.jabong.dap.model.customer.variables

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.variables.{ CustomerVariables, NewsletterVariables, SalesOrderVariables }
import com.jabong.dap.common.schema.SchemaUtils
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.{ Udf, UdfUtils }
import com.jabong.dap.data.storage.merge.common.MergeUtils
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.model.customer.schema.CustVarSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row }

/**
 * Created by raghu on 27/5/15.
 */
object Customer {

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // DataFrame Customer,NLS, SalesOrder operations
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * This method will calculate customer related variable
   * @param dfCustomer
   * @param dfNLS
   * @param dfSalesOrder
   * @param dfPrevVarFull
   * @return (DataFrame, DataFrame)
   */
  def getCustomer(dfCustomer: DataFrame, dfNLS: DataFrame, dfSalesOrder: DataFrame, dfPrevVarFull: DataFrame): (DataFrame, DataFrame) = {

    if (dfCustomer == null || dfNLS == null || dfSalesOrder == null) {

      log("Data frame should not be null")

      return null

    }

    if (!SchemaUtils.isSchemaEqual(dfCustomer.schema, Schema.customer) ||
      !SchemaUtils.isSchemaEqual(dfNLS.schema, Schema.nls) ||
      !SchemaUtils.isSchemaEqual(dfSalesOrder.schema, Schema.salesOrder)) {

      log("schema attributes or data type mismatch")

      return null

    }

    val NLS = dfNLS.select(
      col(NewsletterVariables.EMAIL) as NewsletterVariables.NLS_EMAIL,
      col(NewsletterVariables.STATUS),
      col(NewsletterVariables.UNSUBSCRIBE_KEY),
      col(NewsletterVariables.CREATED_AT) as NewsletterVariables.NLS_CREATED_AT,
      col(NewsletterVariables.UPDATED_AT) as NewsletterVariables.NLS_UPDATED_AT
    )

    //Name of variable: CUSTOMERS PREFERRED ORDER TIMESLOT
    val udfCPOT = getCPOT(dfSalesOrder: DataFrame)

    val dfJoin = dfCustomer.select(
      CustomerVariables.ID_CUSTOMER,
      CustomerVariables.GIFTCARD_CREDITS_AVAILABLE,
      CustomerVariables.STORE_CREDITS_AVAILABLE,
      CustomerVariables.BIRTHDAY,
      CustomerVariables.GENDER,
      CustomerVariables.REWARD_TYPE,
      CustomerVariables.EMAIL,
      CustomerVariables.CREATED_AT,
      CustomerVariables.UPDATED_AT,
      CustomerVariables.FIRST_NAME,
      CustomerVariables.LAST_NAME,
      CustomerVariables.PHONE,
      CustomerVariables.CITY,
      CustomerVariables.IS_CONFIRMED
    )
      .join(NLS, dfCustomer(CustomerVariables.EMAIL) === NLS(NewsletterVariables.NLS_EMAIL), SQL.FULL_OUTER)

      .join(
        dfSalesOrder.select(
          col(SalesOrderVariables.FK_CUSTOMER),
          col(SalesOrderVariables.CREATED_AT) as SalesOrderVariables.SO_CREATED_AT,
          col(SalesOrderVariables.UPDATED_AT) as SalesOrderVariables.SO_UPDATED_AT
        ),
        dfCustomer(CustomerVariables.ID_CUSTOMER) === dfSalesOrder(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER
      )
      .join(udfCPOT, dfCustomer(CustomerVariables.ID_CUSTOMER) === udfCPOT(CustomerVariables.FK_CUSTOMER_CPOT), SQL.FULL_OUTER)

    //Name of variable: EMAIL_SUBSCRIPTION_STATUS
    val udfEmailOptInStatus = udf((nls_email: String, status: String) => getEmailOptInStatus(nls_email: String, status: String))

    /*        Name of variables:
                               ID_CUSTOMER,
                               GIFTCARD_CREDITS_AVAILABLE,
                               STORE_CREDITS_AVAILABLE,
                               EMAIL,
                               BIRTHDAY,
                               GENDER,
                               PLATINUM_STATUS,
                               EMAIL,
                               CREATED_AT,
                               UPDATED_AT,
                               CUSTOMER_ALL_ORDER_TIMESLOT,
                               CUSTOMERS PREFERRED ORDER TIMESLOT,
                               FIRST_NAME,
                               LAST_NAME,
                               PHONE,
                               CITY,
                               VERIFICATION_STATUS,
                               NL_SUB_DATE,
                               UNSUB_KEY,
                               AGE,
                               ACC_REG_DATE,
                               MAX_UPDATED_AT,
                               EMAIL_SUBSCRIPTION_STATUS,
                               */
    val dfInc = dfJoin.select(
      col(CustomerVariables.ID_CUSTOMER),
      col(CustomerVariables.GIFTCARD_CREDITS_AVAILABLE),
      col(CustomerVariables.STORE_CREDITS_AVAILABLE),
      col(CustomerVariables.BIRTHDAY),
      col(CustomerVariables.GENDER),
      col(CustomerVariables.REWARD_TYPE),
      col(CustomerVariables.EMAIL),
      col(CustomerVariables.CREATED_AT),
      col(CustomerVariables.UPDATED_AT),
      col(CustomerVariables.CUSTOMER_ALL_ORDER_TIMESLOT),
      col(CustomerVariables.CUSTOMER_PREFERRED_ORDER_TIMESLOT),
      col(CustomerVariables.FIRST_NAME),
      col(CustomerVariables.LAST_NAME),
      col(CustomerVariables.PHONE),
      col(CustomerVariables.CITY),
      col(CustomerVariables.IS_CONFIRMED) as CustomerVariables.VERIFICATION_STATUS,
      col(NewsletterVariables.NLS_CREATED_AT) as NewsletterVariables.NL_SUB_DATE,
      col(NewsletterVariables.UNSUBSCRIBE_KEY) as NewsletterVariables.UNSUB_KEY,

      Udf.age(dfJoin(CustomerVariables.BIRTHDAY)) as CustomerVariables.AGE,

      Udf.minTimestamp(
        dfJoin(CustomerVariables.CREATED_AT),
        dfJoin(NewsletterVariables.NLS_CREATED_AT)
      ) as CustomerVariables.REG_DATE,

      Udf.maxTimestamp(
        dfJoin(CustomerVariables.UPDATED_AT),
        Udf.maxTimestamp(
          dfJoin(NewsletterVariables.NLS_UPDATED_AT),
          dfJoin(SalesOrderVariables.SO_UPDATED_AT)
        )
      )
        as CustomerVariables.LAST_UPDATED_AT,

      udfEmailOptInStatus(
        dfJoin(NewsletterVariables.NLS_EMAIL),
        dfJoin(NewsletterVariables.STATUS)
      ) as CustomerVariables.EMAIL_SUBSCRIPTION_STATUS
    )

    var dfFull: DataFrame = dfInc

    if (null != dfPrevVarFull) {

      //join old and new data frame
      val joinDF = MergeUtils.joinOldAndNewDF(dfInc, dfPrevVarFull, CustomerVariables.ID_CUSTOMER)

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
   * EMAIL_SUBSCRIPTION_STATUS
   * iou - i: opt in(subscribed), o: opt out(when registering they have opted out), u: unsubscribed
   * @param nls_email
   * @param status
   * @return String
   */
  def getEmailOptInStatus(nls_email: String, status: String): String = {

    if (nls_email == null) {
      return "O"
    }

    status match {
      case "subscribed" => "I"
      case "unsubscribed" => "U"
    }

  }

  /**
   * CustomersPreferredOrderTimeslot: Time slot: 2 hrs each, start from 7 am. total 12 slots (1 to 12)
   * @param dfSalesOrder
   * @return DataFrame
   */
  def getCPOT(dfSalesOrder: DataFrame): DataFrame = {

    val salesOrder = dfSalesOrder.select(SalesOrderVariables.FK_CUSTOMER, SalesOrderVariables.CREATED_AT)
      .sort(SalesOrderVariables.FK_CUSTOMER, SalesOrderVariables.CREATED_AT)

    val soMapReduce = salesOrder.map(r => ((r(0), TimeUtils.timeToSlot(r(1).toString, TimeConstants.DATE_TIME_FORMAT)), 1)).reduceByKey(_ + _)

    val soNewMap = soMapReduce.map{ case (key, value) => (key._1, (key._2.asInstanceOf[Int], value.toInt)) }

    val soGrouped = soNewMap.groupByKey()

    val finalData = soGrouped.map{ case (key, value) => (key.toString, UdfUtils.getCompleteSlotData(value)) }

    val rowRDD = finalData.map({ case (key, value) => Row(key.toInt, value._1, value._2) })

    // Apply the schema to the RDD.
    val df = Spark.getSqlContext().createDataFrame(rowRDD, CustVarSchema.customersPreferredOrderTimeslot)

    df
  }

}