package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 20/10/15.
 */
object PaybackData {

  def start(params: ParamInfo) = {

    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val paths = OptionUtils.getOptValue(params.path)

    val (salesOrder, paymentPrepaidTransactionData, paymentBankPriority, soPaybackEarn, soPaybackRedeem) = readDF(paths, incrDate)

    val (dfPaybackData) = getPaybackData(salesOrder, paymentPrepaidTransactionData, paymentBankPriority, soPaybackEarn, soPaybackRedeem)

    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    DataWriter.writeCsv(dfPaybackData.na.fill(""), DataSets.VARIABLES, DataSets.PAYBACK_DATA, DataSets.DAILY_MODE, incrDate, "53699_78384_" + fileDate + "_payback_data", DataSets.IGNORE_SAVEMODE, "true", ";")

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
  def getPaybackData(salesOrder: DataFrame, paymentPrepaidTransactionData: DataFrame, paymentBankPriority: DataFrame, soPaybackEarn: DataFrame, soPaybackRedeem: DataFrame): (DataFrame) = {
    null
  }

  /**
   *
   * @param paths
   * @param incrDate
   * @return
   */
  def readDF(paths: String, incrDate: String): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {

    if (paths != null) {

      val dfSalesOrder = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.FULL_MERGE_MODE, incrDate)
      val dfPaymentPrepaidTransactionData = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.PAYMENT_PREPAID_TRANSACTION_DATA, DataSets.FULL_MERGE_MODE, incrDate)
      val dfPaymentBankPriority = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.PAYMENT_BANK_PRIORITY, DataSets.FULL_MERGE_MODE, incrDate)
      val dfPaybackEarn = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_PAYBACK_EARN, DataSets.FULL_MERGE_MODE, incrDate)
      val dfPaybackRedeem = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_PAYBACK_REDEEM, DataSets.FULL_MERGE_MODE, incrDate)

      (dfSalesOrder, dfPaymentPrepaidTransactionData, dfPaymentBankPriority, dfPaybackEarn, dfPaybackRedeem)
    } else {

      val dfSalesOrder = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.DAILY_MODE, incrDate)
      val dfPaymentPrepaidTransactionData = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.PAYMENT_PREPAID_TRANSACTION_DATA, DataSets.DAILY_MODE, incrDate)
      val dfPaymentBankPriority = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.PAYMENT_BANK_PRIORITY, DataSets.DAILY_MODE, incrDate)
      val dfPaybackEarn = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_PAYBACK_EARN, DataSets.DAILY_MODE, incrDate)
      val dfPaybackRedeem = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_PAYBACK_REDEEM, DataSets.DAILY_MODE, incrDate)

      (dfSalesOrder, dfPaymentPrepaidTransactionData, dfPaymentBankPriority, dfPaybackEarn, dfPaybackRedeem)
    }
  }

}
