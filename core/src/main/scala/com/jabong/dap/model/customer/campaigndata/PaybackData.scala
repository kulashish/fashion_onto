package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ ContactListMobileVars, CustomerVariables, SalesOrderVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.dataFeeds.DataFeedsModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable.HashMap

/**
 * Created by raghu on 20/10/15.
 */
object PaybackData extends DataFeedsModel {

  val ICICI_DEBITCARD = "icici_debitcard"
  val EARN_POINTS = "earn_points"
  val BURN_POINTS = "burn_points"
  val PAYBACK = "payback"
  val OLD_ = "old_"
  val BANK_CODE = "bank_code"

  def canProcess(incrDate: String, saveMode: String): Boolean = {
    val pathPaybackDataFull = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.PAYBACK_DATA, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.canWrite(saveMode, pathPaybackDataFull)
  }

  /**
   *
   * @param paths
   * @param incrDate
   * @return
   */
  def readDF(incrDate: String, prevDate: String, paths: String): HashMap[String, DataFrame] = {
    val dateDiffFormat = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_FORMAT)

    var dfMap = new HashMap[String, DataFrame]()

    val paymentBankPriorityFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.PAYMENT_BANK_PRIORITY, DataSets.FULL_FETCH_MODE, dateDiffFormat)
    dfMap.put("paymentBankPriorityFull", paymentBankPriorityFull)

    val cmrFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, incrDate)
    dfMap.put("cmrFull", cmrFull)

    var mode = DataSets.FULL_MERGE_MODE

    if (paths == null) {
      mode = DataSets.DAILY_MODE
      val paybackDataPrevFull = DataReader.getDataFrame(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.PAYBACK_DATA, DataSets.FULL_MERGE_MODE, prevDate)
      dfMap.put("paybackDataPrevFull", paybackDataPrevFull)
    }
    val salesOrderIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, mode, incrDate)
    dfMap.put("salesOrderIncr", salesOrderIncr)
    val paymentPrepaidTransactionDataIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.PAYMENT_PREPAID_TRANSACTION_DATA, mode, incrDate)
    dfMap.put("paymentPrepaidTransactionDataIncr", paymentPrepaidTransactionDataIncr)
    val paybackEarnIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_PAYBACK_EARN, mode, incrDate)
    dfMap.put("paybackEarnIncr", paybackEarnIncr)
    val paybackRedeemIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_PAYBACK_REDEEM, mode, incrDate)
    dfMap.put("paybackRedeemIncr", paybackRedeemIncr)
    dfMap
  }

  def process(dfMap: HashMap[String, DataFrame]): HashMap[String, DataFrame] = {
    val salesOrderIncr = dfMap("salesOrderIncr")
    val paymentPrepaidTransactionDataIncr = dfMap("paymentPrepaidTransactionDataIncr")
    val paymentBankPriorityFull = dfMap("paymentBankPriorityFull")
    val paybackEarnIncr = dfMap("paybackEarnIncr")
    val paybackRedeemIncr = dfMap("paybackRedeemIncr")
    val cmrFull = dfMap("salesOrderIncr")
    val paybackDataPrevFull = dfMap("cmrFull")

    val dfIcici = salesOrderIncr.join(paymentPrepaidTransactionDataIncr, salesOrderIncr(SalesOrderVariables.ID_SALES_ORDER) === paymentPrepaidTransactionDataIncr(SalesOrderVariables.FK_SALES_ORDER), SQL.INNER)
      .join(paymentBankPriorityFull, paymentPrepaidTransactionDataIncr(BANK_CODE) === paymentBankPriorityFull(BANK_CODE), SQL.INNER)
      .select(
        salesOrderIncr(SalesOrderVariables.FK_CUSTOMER) as CustomerVariables.ID_CUSTOMER,
        lit(1) as ICICI_DEBITCARD
      )

    val dfEarn = salesOrderIncr.join(paybackEarnIncr, salesOrderIncr(SalesOrderVariables.ID_SALES_ORDER) === paybackEarnIncr(SalesOrderVariables.FK_SALES_ORDER), SQL.INNER)
      .select(
        salesOrderIncr(SalesOrderVariables.FK_CUSTOMER),
        lit(1) as EARN_POINTS
      )

    val dfBurn = salesOrderIncr.join(paybackRedeemIncr, salesOrderIncr(SalesOrderVariables.ID_SALES_ORDER) === paybackRedeemIncr(SalesOrderVariables.FK_SALES_ORDER), SQL.INNER)
      .select(
        salesOrderIncr(SalesOrderVariables.FK_CUSTOMER),
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

    val dfCmr = cmrFull.select(
      cmrFull(ContactListMobileVars.UID),
      cmrFull(CustomerVariables.ID_CUSTOMER)
    )

    val dfInc = dfIciciEarnBurn.join(dfCmr, dfCmr(CustomerVariables.ID_CUSTOMER) === dfIciciEarnBurn(CustomerVariables.ID_CUSTOMER), SQL.INNER)
      .select(
        dfCmr(ContactListMobileVars.UID),
        dfIciciEarnBurn(ICICI_DEBITCARD),
        dfIciciEarnBurn(EARN_POINTS),
        dfIciciEarnBurn(BURN_POINTS),
        lit(1) as PAYBACK
      ).distinct

    var paybackIncr = dfInc
    var paybackDataFull = dfInc

    if (null != paybackDataPrevFull) {

      val prevPayback = paybackDataPrevFull.select(ContactListMobileVars.UID)

      paybackIncr = dfInc.join(prevPayback, dfInc(ContactListMobileVars.UID) === prevPayback(ContactListMobileVars.UID), SQL.LEFT_OUTER)
        .filter(prevPayback(ContactListMobileVars.UID).isNull)
        .select(dfInc("*"))

      paybackDataFull = paybackDataPrevFull.unionAll(paybackIncr)
    }

    var dfWrite = new HashMap[String, DataFrame]()
    dfWrite.put("paybackIncr", paybackIncr)
    dfWrite.put("paybackDataFull", paybackDataFull)
    dfWrite
  }

  def write(dfWrite: HashMap[String, DataFrame], saveMode: String, incrDate: String) = {
    val pathPaybackDataFull = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.PAYBACK_DATA, DataSets.FULL_MERGE_MODE, incrDate)
    if (DataWriter.canWrite(saveMode, pathPaybackDataFull)) {
      DataWriter.writeParquet(dfWrite("paybackDataFull"), pathPaybackDataFull, saveMode)
    }

    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    DataWriter.writeCsv(dfWrite("paybackIncr").na.fill(""), DataSets.VARIABLES, DataSets.PAYBACK_DATA, DataSets.DAILY_MODE, incrDate, fileDate + "_payback_data", DataSets.IGNORE_SAVEMODE, "true", ";")
  }
}
