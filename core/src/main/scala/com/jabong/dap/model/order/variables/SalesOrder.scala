package com.jabong.dap.model.order.variables

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.variables.{ContactListMobileVars, SalesOrderVariables}
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.UdfUtils
import com.jabong.dap.model.customer.schema.CustVarSchema
import org.apache.spark.sql.{ Row, DataFrame }
import org.apache.spark.sql.functions._

/**
 * Created by jabong on 24/6/15.
 */
object SalesOrder {

  /**
   *
   * @param salesOrders
   * @return
   */
  def couponScore(salesOrders: DataFrame): DataFrame = {
    val salesOrderNew = salesOrders.select(SalesOrderVariables.FK_CUSTOMER, SalesOrderVariables.COUPON_CODE).na.drop()
    val couponScore = salesOrderNew.groupBy(SalesOrderVariables.FK_CUSTOMER).agg(count(SalesOrderVariables.COUPON_CODE) as SalesOrderVariables.COUPON_SCORE)
    return couponScore
  }

  def processVariables(salesOrderCalcFull: DataFrame, salesOrderIncr: DataFrame): DataFrame = {
    val salesOrderCalcIncr = salesOrderIncr.groupBy(SalesOrderVariables.FK_CUSTOMER).agg(
      max(SalesOrderVariables.CREATED_AT) as ContactListMobileVars.LAST_ORDER_DATE,
      max(SalesOrderVariables.UPDATED_AT) as SalesOrderVariables.UPDATED_AT,
      min(SalesOrderVariables.CREATED_AT) as SalesOrderVariables.FIRST_ORDER_DATE,
      count(SalesOrderVariables.CREATED_AT) as SalesOrderVariables.ORDERS_COUNT,
      count(SalesOrderVariables.CREATED_AT) - count(SalesOrderVariables.CREATED_AT) as SalesOrderVariables.DAYS_SINCE_LAST_ORDER
    )
    if (null == salesOrderCalcFull) {
      salesOrderCalcIncr
    } else {
      val joinedDF = salesOrderCalcFull.unionAll(salesOrderCalcIncr)
      val salesOrderCalcNewFull = joinedDF.groupBy(SalesOrderVariables.FK_CUSTOMER).agg(
        max(ContactListMobileVars.LAST_ORDER_DATE) as ContactListMobileVars.LAST_ORDER_DATE,
        max(SalesOrderVariables.UPDATED_AT) as SalesOrderVariables.UPDATED_AT,
        min(SalesOrderVariables.FIRST_ORDER_DATE) as SalesOrderVariables.FIRST_ORDER_DATE,
        sum(SalesOrderVariables.ORDERS_COUNT) as SalesOrderVariables.ORDERS_COUNT,
        min(SalesOrderVariables.DAYS_SINCE_LAST_ORDER) + 1 as SalesOrderVariables.DAYS_SINCE_LAST_ORDER
      )
      salesOrderCalcNewFull
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
