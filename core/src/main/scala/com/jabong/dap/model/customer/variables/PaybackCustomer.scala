package com.jabong.dap.model.customer.variables

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.variables.{ PaybackCustomerVariables, SalesOrderVariables }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import grizzled.slf4j.Logging
import com.jabong.dap.data.acq.common.DaoUtil
/**
 * Created by Kapil.Rajak on 1/7/15.
 */
object PaybackCustomer extends Logging {

  def getIncrementalPaybackCustomer(dfSalesOrder: DataFrame, dfPaybackEarn: DataFrame, dfPaybackRedeem: DataFrame): DataFrame = {
    logger.info("Executing getIncrementalPaybackCustomer.")
    val payBackCustomersEarn = dfSalesOrder.select(SalesOrderVariables.FK_CUSTOMER, SalesOrderVariables.ID_SALES_ORDER)
      .join(dfPaybackEarn.select(PaybackCustomerVariables.FK_SALES_ORDER), dfSalesOrder(SalesOrderVariables.ID_SALES_ORDER) === dfPaybackEarn(PaybackCustomerVariables.FK_SALES_ORDER)) //dfPaybackEarn.select(CustomerLoyaltyVariables.FK_CUSTOMER).withColumn(CustomerLoyaltyVariables.IS_PAYBACK, col(CustomerLoyaltyVariables.FK_CUSTOMER) || true)
      .select(col(SalesOrderVariables.FK_CUSTOMER))
      .withColumn(PaybackCustomerVariables.IS_PAYBACK, col(PaybackCustomerVariables.FK_CUSTOMER) || true)
    val payBackCustomersRedeem = dfPaybackRedeem.select(PaybackCustomerVariables.FK_CUSTOMER).withColumn(PaybackCustomerVariables.IS_PAYBACK, col(PaybackCustomerVariables.FK_CUSTOMER) || true)

    payBackCustomersEarn.unionAll(payBackCustomersRedeem).distinct
  }

  def getMergedPaybackCustomer(dfCurr: DataFrame, dfPrev: DataFrame): DataFrame = {
    logger.info("Executing getMergedPaybackCustomer")

    dfPrev.unionAll(dfCurr).distinct
  }

  def getPaybackCustomer(dfSalesOrder: DataFrame, dfPaybackEarn: DataFrame, dfPaybackRedeem: DataFrame, dfPrevCalculated: DataFrame): (DataFrame, DataFrame) = {
    logger.info("Executing getPaybackCustomer.")

    val dfIncrementalPaybackCustomer = getIncrementalPaybackCustomer(dfSalesOrder: DataFrame, dfPaybackEarn: DataFrame, dfPaybackRedeem: DataFrame)
    val dfMergedPaybackCustomer = getMergedPaybackCustomer(dfIncrementalPaybackCustomer, dfPrevCalculated)

    (dfIncrementalPaybackCustomer, dfMergedPaybackCustomer)
  }
}