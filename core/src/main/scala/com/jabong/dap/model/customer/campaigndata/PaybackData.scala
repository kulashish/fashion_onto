package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ ContactListMobileVars, CustomerVariables, SalesOrderVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 20/10/15.
 */
object PaybackData {

  val ICICI_DEBITCARD = "icici_debitcard"
  val EARN_POINTS = "earn_points"
  val BURN_POINTS = "burn_points"
  val PAYBACK = "payback"
  val OLD_ = "old_"
  val BANK_CODE = "bank_code"

  def start(params: ParamInfo) = {

    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val saveMode = params.saveMode
    val paths = OptionUtils.getOptValue(params.path)
    val prevDate = OptionUtils.getOptValue(params.fullDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER, incrDate))

    val (salesOrder, paymentPrepaidTransactionData, paymentBankPriority, soPaybackEarn, soPaybackRedeem, dfCmrFull, prevFullPayback) = readDF(paths, incrDate, prevDate)

    val (incPaybackData, fullPaybackData) = getPaybackData(salesOrder, paymentPrepaidTransactionData, paymentBankPriority, soPaybackEarn, soPaybackRedeem, dfCmrFull, prevFullPayback)

    val pathPaybackDataFull = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.PAYBACK_DATA, DataSets.FULL_MERGE_MODE, incrDate)
    if (DataWriter.canWrite(saveMode, pathPaybackDataFull)) {
      DataWriter.writeParquet(fullPaybackData, pathPaybackDataFull, saveMode)
    }

    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    DataWriter.writeCsv(incPaybackData.na.fill(""), DataSets.VARIABLES, DataSets.PAYBACK_DATA, DataSets.DAILY_MODE, incrDate, fileDate + "_payback_data", DataSets.IGNORE_SAVEMODE, "true", ";")

  }

  /**
   *
   * @param salesOrder
   * @param paymentPrepaidTransactionData
   * @param paymentBankPriority
   * @param soPaybackEarn
   * @param soPaybackRedeem
   * @return
   */
  def getPaybackData(salesOrder: DataFrame, paymentPrepaidTransactionData: DataFrame, paymentBankPriority: DataFrame, soPaybackEarn: DataFrame, soPaybackRedeem: DataFrame, dfCmrFull: DataFrame, prevFullPayback: DataFrame): (DataFrame, DataFrame) = {

    val dfIcici = salesOrder.join(paymentPrepaidTransactionData, salesOrder(SalesOrderVariables.ID_SALES_ORDER) === paymentPrepaidTransactionData(SalesOrderVariables.FK_SALES_ORDER), SQL.INNER)
      .join(paymentBankPriority, paymentPrepaidTransactionData(BANK_CODE) === paymentBankPriority(BANK_CODE), SQL.INNER)
      .select(
        salesOrder(SalesOrderVariables.FK_CUSTOMER) as CustomerVariables.ID_CUSTOMER,
        lit(1) as ICICI_DEBITCARD
      )

    val dfEarn = salesOrder.join(soPaybackEarn, salesOrder(SalesOrderVariables.ID_SALES_ORDER) === soPaybackEarn(SalesOrderVariables.FK_SALES_ORDER), SQL.INNER)
      .select(
        salesOrder(SalesOrderVariables.FK_CUSTOMER),
        lit(1) as EARN_POINTS
      )

    val dfBurn = salesOrder.join(soPaybackRedeem, salesOrder(SalesOrderVariables.ID_SALES_ORDER) === soPaybackRedeem(SalesOrderVariables.FK_SALES_ORDER), SQL.INNER)
      .select(
        salesOrder(SalesOrderVariables.FK_CUSTOMER),
        lit(1) as BURN_POINTS
      )

    val dfIciciEarn = dfIcici.join(dfEarn, dfIcici(CustomerVariables.ID_CUSTOMER) === dfEarn(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
      .select(
        coalesce(dfIcici(CustomerVariables.ID_CUSTOMER), dfEarn(SalesOrderVariables.FK_CUSTOMER)) as CustomerVariables.ID_CUSTOMER,
        dfIcici(ICICI_DEBITCARD),
        dfEarn(EARN_POINTS)
      )

    val dfIciciEarnBurn = dfIciciEarn.join(dfBurn, dfIciciEarn(CustomerVariables.ID_CUSTOMER) === dfBurn(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
      .select(
        coalesce(dfIciciEarn(CustomerVariables.ID_CUSTOMER), dfBurn(SalesOrderVariables.FK_CUSTOMER)) as CustomerVariables.ID_CUSTOMER,
        dfIciciEarn(ICICI_DEBITCARD),
        dfIciciEarn(EARN_POINTS),
        dfBurn(BURN_POINTS)
      ).na.fill(0)

    val dfCmr = dfCmrFull.select(
      dfCmrFull(ContactListMobileVars.UID),
      dfCmrFull(CustomerVariables.ID_CUSTOMER)
    )

    val dfInc = dfIciciEarnBurn.join(dfCmr, dfCmr(CustomerVariables.ID_CUSTOMER) === dfIciciEarnBurn(CustomerVariables.ID_CUSTOMER), SQL.INNER)
      .select(
        dfCmr(ContactListMobileVars.UID),
        dfIciciEarnBurn(ICICI_DEBITCARD),
        dfIciciEarnBurn(EARN_POINTS),
        dfIciciEarnBurn(BURN_POINTS),
        lit(1) as PAYBACK
      ).distinct

    if (prevFullPayback != null) {

      val prevPayback = prevFullPayback.select(ContactListMobileVars.UID)

      val df = dfInc.join(prevPayback, dfInc(ContactListMobileVars.UID) === prevPayback(ContactListMobileVars.UID), SQL.LEFT_OUTER)
        .filter(prevPayback(ContactListMobileVars.UID).isNull)
        .select(dfInc("*"))

      return (df, prevFullPayback.unionAll(df))
    }

    return (dfInc, dfInc)
  }

  /**
   *
   * @param paths
   * @param incrDate
   * @return
   */
  def readDF(paths: String, incrDate: String, prevDate: String): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {

    val dateDiffFormat = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_FORMAT)

    val dfPaymentBankPriority = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.PAYMENT_BANK_PRIORITY, DataSets.FULL_FETCH_MODE, dateDiffFormat)
    val dfCmrFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, incrDate)

    if (paths != null) {

      val dfSalesOrder = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.FULL_MERGE_MODE, incrDate)
      val dfPaymentPrepaidTransactionData = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.PAYMENT_PREPAID_TRANSACTION_DATA, DataSets.FULL_MERGE_MODE, incrDate)
      val dfPaybackEarn = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_PAYBACK_EARN, DataSets.FULL_MERGE_MODE, incrDate)
      val dfPaybackRedeem = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_PAYBACK_REDEEM, DataSets.FULL_MERGE_MODE, incrDate)

      (dfSalesOrder, dfPaymentPrepaidTransactionData, dfPaymentBankPriority, dfPaybackEarn, dfPaybackRedeem, dfCmrFull, null)
    } else {

      val dfSalesOrder = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.DAILY_MODE, incrDate)
      val dfPaymentPrepaidTransactionData = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.PAYMENT_PREPAID_TRANSACTION_DATA, DataSets.DAILY_MODE, incrDate)
      val dfPaybackEarn = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_PAYBACK_EARN, DataSets.DAILY_MODE, incrDate)
      val dfPaybackRedeem = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_PAYBACK_REDEEM, DataSets.DAILY_MODE, incrDate)

      val dfPrivFullPayback = DataReader.getDataFrame(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.PAYBACK_DATA, DataSets.FULL_MERGE_MODE, prevDate)

      (dfSalesOrder, dfPaymentPrepaidTransactionData, dfPaymentBankPriority, dfPaybackEarn, dfPaybackRedeem, dfCmrFull, dfPrivFullPayback)
    }
  }

}