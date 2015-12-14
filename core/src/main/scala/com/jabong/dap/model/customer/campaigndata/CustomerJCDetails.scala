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
object CustomerJCDetails {

  def start(params: ParamInfo) = {

    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val paths = OptionUtils.getOptValue(params.path)

    val (dfCustomer, dfCustomerStorecreditsHistory) = readDF(paths, incrDate)

    val (dfCustomerJCDetails) = getCustomerJCDetails(dfCustomer, dfCustomerStorecreditsHistory)

    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    DataWriter.writeCsv(dfCustomerJCDetails.na.fill(""), DataSets.VARIABLES, DataSets.CUSTOMER_JC_DETAILS, DataSets.DAILY_MODE, incrDate, fileDate + "_Customer_JC_details", DataSets.IGNORE_SAVEMODE, "true", ";", 1)

  }

  /**
   *
   * @param dfCustomer
   * @param dfCustomerStorecreditsHistory
   * @return
   */
  def getCustomerJCDetails(dfCustomer: DataFrame, dfCustomerStorecreditsHistory: DataFrame): (DataFrame) = {
    null
  }

  /**
   *
   * @param paths
   * @param incrDate
   * @return
   */
  def readDF(paths: String, incrDate: String): (DataFrame, DataFrame) = {

    if (paths != null) {

      val dfCustomer = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CUSTOMER, DataSets.FULL_MERGE_MODE, incrDate)
      val dfCustomerStorecreditsHistory = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CUSTOMER_STORECREDITS_HISTORY, DataSets.FULL_MERGE_MODE, incrDate)

      (dfCustomer, dfCustomerStorecreditsHistory)
    } else {

      val dfCustomer = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CUSTOMER, DataSets.DAILY_MODE, incrDate)
      val dfCustomerStorecreditsHistory = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CUSTOMER_STORECREDITS_HISTORY, DataSets.DAILY_MODE, incrDate)

      (dfCustomer, dfCustomerStorecreditsHistory)
    }
  }

}
