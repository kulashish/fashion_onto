package com.jabong.dap.model.customer.variables

import java.sql.Timestamp

import com.jabong.dap.common.constants.variables.{ SalesOrderVariables, NewsletterVariables, CustomerVariables }
import com.jabong.dap.common.utils.Time
import com.jabong.dap.common.{ Constants, Spark, Utils }
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.model.schema.SchemaVariables
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row }

/**
 * Created by raghu on 27/5/15.
 */
object Customer {

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // DataFrame Customer,NLS, SalesOrder operations
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /*        Name of variables:id_customer,
                                  GIFTCARD_CREDITS_AVAILABLE,
                                  STORE_CREDITS_AVAILABLE,
                                  EMAIL,
                                  BIRTHDAY,
                                  GENDER,
                                  PLATINUM_STATUS,
                                  ACC_REG_DATE,
                                  UPDATED_AT,
                                  EMAIL_OPT_IN_STATUS,
                                  CUSTOMERS PREFERRED ORDER TIMESLOT*/
  def getCustomer(dfCustomer: DataFrame, dfNLS: DataFrame, dfSalesOrder: DataFrame): DataFrame = {

    if (dfCustomer == null || dfNLS == null || dfSalesOrder == null) {

      log("Data frame should not be null")

      return null

    }

    if (!Utils.isSchemaEqual(dfCustomer.schema, Schema.customer) ||
      !Utils.isSchemaEqual(dfNLS.schema, Schema.nls) ||
      !Utils.isSchemaEqual(dfSalesOrder.schema, Schema.salesOrder)) {

      log("schema attributes or data type mismatch")

      return null

    }

    val NLS = dfNLS.select(
      col(NewsletterVariables.EMAIL) as NewsletterVariables.NLS_EMAIL,
      col(NewsletterVariables.STATUS),
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
      CustomerVariables.UPDATED_AT
    )

      .join(NLS, dfCustomer(CustomerVariables.EMAIL) === NLS(NewsletterVariables.NLS_EMAIL), "outer")

      .join(
        dfSalesOrder.select(
        col(SalesOrderVariables.FK_CUSTOMER),
        col(SalesOrderVariables.CREATED_AT) as SalesOrderVariables.SO_CREATED_AT,
        col(SalesOrderVariables.UPDATED_AT) as SalesOrderVariables.SO_UPDATED_AT
      ),
        dfCustomer(CustomerVariables.ID_CUSTOMER) === dfSalesOrder(SalesOrderVariables.FK_CUSTOMER), "outer"
      )

      .join(udfCPOT, dfCustomer(CustomerVariables.ID_CUSTOMER) === udfCPOT(CustomerVariables.FK_CUSTOMER_CPOT), "outer")

    // Define User Defined Functions
    val sqlContext = Spark.getSqlContext()

    //min(customer.created_at, sales_order.created_at)
    val udfAccRegDate = udf((cust_created_at: Timestamp, nls_created_at: Timestamp) => getMin(cust_created_at: Timestamp, nls_created_at: Timestamp))

    //max(customer.updated_at, newsletter_subscription.updated_at, sales_order.updated_at)
    val udfMaxUpdatedAt = udf((cust_updated_at: Timestamp, nls_updated_at: Timestamp, so_updated_at: Timestamp) => getMax(getMax(cust_updated_at: Timestamp, nls_updated_at: Timestamp), so_updated_at: Timestamp))

    //Name of variable: EMAIL_OPT_IN_STATUS
    val udfEmailOptInStatus = udf((nls_email: String, status: String) => getEmailOptInStatus(nls_email: String, status: String))

    /*        Name of variables:ID_CUSTOMER,
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
                                     ACC_REG_DATE,
                                     MAX_UPDATED_AT,
                                     EMAIL_OPT_IN_STATUS,*/
    val dfResult = dfJoin.select(
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

      udfAccRegDate(
        dfJoin(CustomerVariables.CREATED_AT),
        dfJoin(NewsletterVariables.NLS_CREATED_AT)
      ) as CustomerVariables.ACC_REG_DATE,

      udfMaxUpdatedAt(
        dfJoin(CustomerVariables.UPDATED_AT),
        dfJoin(NewsletterVariables.NLS_CREATED_AT),
        dfJoin(SalesOrderVariables.SO_CREATED_AT)
      ) as CustomerVariables.MAX_UPDATED_AT,

      udfEmailOptInStatus(
        dfJoin(NewsletterVariables.NLS_EMAIL),
        dfJoin(NewsletterVariables.STATUS)
      ) as CustomerVariables.EMAIL_OPT_IN_STATUS
    )

    //
    //          if (isOldDate) {
    //
    //            val dfCustomerFull = Spark.getSqlContext().read.parquet(DataFiles.VARIABLE_PATH + DataFiles.CUSTOMER + "/full" + oldDateFolder)
    //
    //            val custBCVar = Spark.getContext().broadcast(dfResult)
    //
    //            dfResult = MergeDataImpl.InsertUpdateMerge(dfCustomerFull, custBCVar.value, "id_customer")
    //          }

    dfResult
  }

  //min(customer.created_at, sales_order.created_at)
  def getMin(t1: Timestamp, t2: Timestamp): Timestamp = {

    if (t1 == null) {
      return t2
    }

    if (t2 == null) {
      return t1
    }

    if (t1.compareTo(t2) >= 0)
      t1
    else
      t2

  }

  //max(customer.updated_at, newsletter_subscription.updated_at, sales_order.updated_at)
  def getMax(t1: Timestamp, t2: Timestamp): Timestamp = {

    if (t1 == null) {
      return t2
    }

    if (t2 == null) {
      return t1
    }

    if (t1.compareTo(t2) < 0)
      t2
    else
      t1

  }

  //iou - i: opt in(subscribed), o: opt out(when registering they have opted out), u: unsubscribed
  def getEmailOptInStatus(nls_email: String, status: String): String = {

    if (nls_email == null) {
      return "O"
    }

    status match {
      case "subscribed" => "I"
      case "unsubscribed" => "U"
    }

  }

  //CustomersPreferredOrderTimeslot: Time slot: 2 hrs each, start from 7 am. total 12 slots (1 to 12)
  def getCPOT(dfSalesOrder: DataFrame): DataFrame = {

    val salesOrder = dfSalesOrder.select(SalesOrderVariables.FK_CUSTOMER, SalesOrderVariables.CREATED_AT)
      .sort(SalesOrderVariables.FK_CUSTOMER, SalesOrderVariables.CREATED_AT)

    val soMapReduce = salesOrder.map(r => ((r(0), Time.timeToSlot(r(1).toString, Constants.DATE_TIME_FORMAT)), 1)).reduceByKey(_ + _)

    val soNewMap = soMapReduce.map{ case (key, value) => (key._1, (key._2.asInstanceOf[Int], value.toInt)) }

    val soGrouped = soNewMap.groupByKey()

    val finalData = soGrouped.map{ case (key, value) => (key.toString, getCompleteSlotData(value)) }

    val rowRDD = finalData.map({ case (key, value) => Row(key.toInt, value._1, value._2) })

    // Apply the schema to the RDD.
    val df = Spark.getSqlContext().createDataFrame(rowRDD, SchemaVariables.customersPreferredOrderTimeslot)

    df
  }

  def getCompleteSlotData(iterable: Iterable[(Int, Int)]): Tuple2[String, Int] = {

    var timeSlotArray = new Array[Int](13)

    var maxSlot: Int = -1

    var max: Int = -1

    iterable.foreach {
      case (slot, value) =>
        if (value > max) { maxSlot = slot; max = value };
        timeSlotArray(slot) = value
    }
    new Tuple2(arrayToString(timeSlotArray), maxSlot)
  }

  def arrayToString(array: Array[Int]): String = {

    var arrayConverted: String = ""

    for (i <- 1 to array.length - 1) {

      if (i == 1) {
        arrayConverted = array(i).toString
      } else {
        arrayConverted = arrayConverted + "!" + array(i).toString
      }
    }
    arrayConverted
  }

}