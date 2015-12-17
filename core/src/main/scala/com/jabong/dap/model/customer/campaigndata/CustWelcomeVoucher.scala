package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.dataFeeds.DataFeedsModel
import com.jabong.dap.model.order.variables.SalesRule
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.HashMap

/**
 * Created by mubarak on 3/9/15.
 */
object CustWelcomeVoucher extends DataFeedsModel with Logging {

  override def canProcess(incrDate: String, saveMode: String): Boolean = {
    val savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_WELCOME_VOUCHER, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.canWrite(saveMode, savePath)
  }

  override def readDF(incrDate: String, prevDate: String, paths: String): HashMap[String, DataFrame] = {
    val dfMap = new HashMap[String, DataFrame]()
    var salesRuleIncr: DataFrame = null
    var welCodesPrevFull: DataFrame = null

    if (null != paths) {
      salesRuleIncr = DataReader.getDataFrame4mFullPath(paths, DataSets.PARQUET)
    } else {
      salesRuleIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_RULE, DataSets.DAILY_MODE, incrDate)
      welCodesPrevFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_WELCOME_VOUCHER, DataSets.FULL_MERGE_MODE, prevDate)
    }

    val cmrFull = CampaignInput.loadCustomerMasterData(incrDate)

    dfMap.put("salesRuleIncr", salesRuleIncr)
    dfMap.put("welCodesPrevFull", welCodesPrevFull)
    dfMap.put("cmrFull", cmrFull)
    dfMap
  }

  override def process(dfMap: HashMap[String, DataFrame]): HashMap[String, DataFrame] = {
    val salesRuleIncr = dfMap("salesRuleIncr")
    val welCodesPrevFull = dfMap("welCodesPrevFull")

    val welCodesFull = SalesRule.createWcCodes(salesRuleIncr, welCodesPrevFull).cache()
    logger.info("after getting the codes from salesRule Table")

    val dfWrite = new HashMap[String, DataFrame]()
    dfWrite.put("welCodesFull", welCodesFull)
    dfWrite.put("cmrFull", dfMap("cmrFull"))
    dfWrite
  }

  def write(dfWrite: HashMap[String, DataFrame], saveMode: String, incrDate: String) = {
    val welCodesFull = dfWrite("welCodesFull")
    val cmrFull = dfWrite("cmrFull")

    val savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_WELCOME_VOUCHER, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(welCodesFull, savePath, saveMode)

    val res = welCodesFull.join(cmrFull, welCodesFull(SalesRuleVariables.FK_CUSTOMER) === cmrFull(CustomerVariables.ID_CUSTOMER))
      .filter((welCodesFull(SalesRuleVariables.CODE1_VALID_DATE).isNotNull && welCodesFull(SalesRuleVariables.CODE1_VALID_DATE).geq(TimeUtils.getTimeStamp()))
        || (welCodesFull(SalesRuleVariables.CODE2_VALID_DATE).isNotNull && welCodesFull(SalesRuleVariables.CODE2_VALID_DATE).geq(TimeUtils.getTimeStamp())))
      .select(
        cmrFull(ContactListMobileVars.UID),
        welCodesFull(SalesRuleVariables.CODE1),
        Udf.dateCsvFormat(welCodesFull(SalesRuleVariables.CODE1_CREATION_DATE)) as SalesRuleVariables.CODE1_CREATION_DATE,
        Udf.dateCsvFormat(welCodesFull(SalesRuleVariables.CODE1_VALID_DATE)) as SalesRuleVariables.CODE1_VALID_DATE,
        welCodesFull(SalesRuleVariables.CODE2),
        Udf.dateCsvFormat(welCodesFull(SalesRuleVariables.CODE2_CREATION_DATE)) as SalesRuleVariables.CODE2_CREATION_DATE,
        Udf.dateCsvFormat(welCodesFull(SalesRuleVariables.CODE2_VALID_DATE)) as SalesRuleVariables.CODE2_VALID_DATE)
      .na.fill("")
    logger.info("after filter on date")
    val savePathIncr = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_WELCOME_VOUCHER, DataSets.DAILY_MODE, incrDate)
    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    DataWriter.writeParquet(res, savePathIncr, saveMode)
    DataWriter.writeCsv(res, DataSets.VARIABLES, DataSets.CUST_WELCOME_VOUCHER, DataSets.DAILY_MODE, incrDate, fileDate + "_CUST_WELCOME_VOUCHERS", DataSets.IGNORE_SAVEMODE, "true", ";", 1)
  }
}