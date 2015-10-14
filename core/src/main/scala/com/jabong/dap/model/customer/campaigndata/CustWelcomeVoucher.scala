package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.order.variables.SalesRule
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * Created by mubarak on 3/9/15.
 */
object CustWelcomeVoucher extends Logging {

  def start(vars: ParamInfo) = {

    val saveMode = vars.saveMode
    val fullpath = OptionUtils.getOptValue(vars.path)
    val incrDate = OptionUtils.getOptValue(vars.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val prevDate = OptionUtils.getOptValue(vars.fullDate, TimeUtils.getDateAfterNDays(-2, TimeConstants.DATE_FORMAT_FOLDER))

    val savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_WELCOME_VOUCHER, DataSets.FULL_MERGE_MODE, incrDate)

    if (DataWriter.canWrite(saveMode, savePath)) {

      val (salesRuleIncr, welCodesPrevFull, cmrFull) = readDf(incrDate, prevDate, fullpath)

      val welCodes = SalesRule.createWcCodes(salesRuleIncr, welCodesPrevFull).cache()
      logger.info("after getting the codes from salesRule Table")

      //TODO add UID
      DataWriter.writeParquet(welCodes, savePath, saveMode)

      val res = welCodes.join(cmrFull, welCodes(SalesRuleVariables.FK_CUSTOMER) === cmrFull(CustomerVariables.ID_CUSTOMER))
        .select(
          cmrFull(ContactListMobileVars.UID),
          welCodes(SalesRuleVariables.CODE1),
          Udf.dateCsvFormat(welCodes(SalesRuleVariables.CODE1_CREATION_DATE)) as SalesRuleVariables.CODE1_CREATION_DATE,
          Udf.dateCsvFormat(welCodes(SalesRuleVariables.CODE1_VALID_DATE)) as SalesRuleVariables.CODE1_VALID_DATE,
          welCodes(SalesRuleVariables.CODE2),
          Udf.dateCsvFormat(welCodes(SalesRuleVariables.CODE2_CREATION_DATE)) as SalesRuleVariables.CODE2_CREATION_DATE,
          Udf.dateCsvFormat(welCodes(SalesRuleVariables.CODE2_VALID_DATE)) as SalesRuleVariables.CODE2_VALID_DATE)
        .filter(welCodes(SalesRuleVariables.CODE1_VALID_DATE).geq(TimeUtils.getTimeStamp()) && welCodes(SalesRuleVariables.CODE2_VALID_DATE).geq(TimeUtils.getTimeStamp()))
        .na.fill("")
      logger.info("after filter on date")

      val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
      DataWriter.writeCsv(res, DataSets.VARIABLES, DataSets.CUST_WELCOME_VOUCHER, DataSets.DAILY_MODE, incrDate, "53699_28346_" + fileDate + "_CUST_WELCOME_VOUCHERS", DataSets.IGNORE_SAVEMODE, "true", ";")
    }
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

    val cmrFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, prevDate)

    return (dfSalesRuleIncr, dfWelCodesPrevFull, cmrFull)
  }
}