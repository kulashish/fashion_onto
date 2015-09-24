package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ContactListMobileVars, CustomerVariables, SalesRuleVariables}
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.order.variables.SalesRule
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by mubarak on 3/9/15.
 */
object CustWelcomeVoucher extends Logging {

  def start(vars: ParamInfo) = {

    val saveMode = vars.saveMode
    val fullpath = OptionUtils.getOptValue(vars.path)
    val incrDate = OptionUtils.getOptValue(vars.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val prevDate = OptionUtils.getOptValue(vars.fullDate, TimeUtils.getDateAfterNDays(-2, TimeConstants.DATE_FORMAT_FOLDER))

    val (salesRuleIncr, welCodesprevFull, customerFull) = readDf(incrDate, prevDate, fullpath)

    val welCodes = SalesRule.createWcCodes(salesRuleIncr, welCodesprevFull)

    val savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_WELCOME_VOUCHER, DataSets.FULL_MERGE_MODE, incrDate)

    //TODO add UID
    DataWriter.writeParquet(welCodes, savePath, saveMode)

    val date = TimeUtils.getTodayDate(TimeConstants.DATE_TIME_FORMAT)
    val ts = TimeUtils.getTimeStamp(date, TimeConstants.DATE_TIME_FORMAT)
    val res = welCodes.join(customerFull, welCodes(SalesRuleVariables.FK_CUSTOMER) === customerFull(CustomerVariables.ID_CUSTOMER))
      .select(
        coalesce(welCodes(SalesRuleVariables.FK_CUSTOMER), customerFull(CustomerVariables.ID_CUSTOMER)) as ContactListMobileVars.UID,
        customerFull(CustomerVariables.EMAIL) as "EMAIL",
        welCodes(SalesRuleVariables.CODE1),
        welCodes(SalesRuleVariables.CODE1_CREATION_DATE),
        welCodes(SalesRuleVariables.CODE1_VALID_DATE),
        welCodes(SalesRuleVariables.CODE2),
        welCodes(SalesRuleVariables.CODE2_CREATION_DATE),
        welCodes(SalesRuleVariables.CODE2_VALID_DATE))
      .filter(welCodes(SalesRuleVariables.CODE1_VALID_DATE).geq(ts) && welCodes(SalesRuleVariables.CODE2_VALID_DATE).geq(ts))

    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    DataWriter.writeCsv(res, ConfigConstants.WRITE_OUTPUT_PATH, DataSets.CUST_PREFERENCE, DataSets.FULL_MERGE_MODE, incrDate, fileDate + "_CUST_WELCOME_VOUCHERS.csv", DataSets.IGNORE_SAVEMODE, "true", ";")

  }

  def readDf(incrDate: String, prevDate: String, fullpath: String): (DataFrame, DataFrame, DataFrame) = {

    var dfSalesRuleIncr: DataFrame = null
    var dfWelCodesPrevFull: DataFrame = null

    if (null != fullpath) {
      dfSalesRuleIncr = DataReader.getDataFrame4mFullPath(fullpath, DataSets.PARQUET)
    } else {
      dfSalesRuleIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_RULE, DataSets.DAILY_MODE, incrDate)
      dfWelCodesPrevFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_WELCOME_VOUCHER, DataSets.FULL_MERGE_MODE, prevDate)
    }

    val customer = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CUSTOMER, DataSets.FULL_MERGE_MODE, prevDate)

    return (dfSalesRuleIncr, dfWelCodesPrevFull, customer)
  }
}