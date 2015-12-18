package com.jabong.dap.model.customer.data

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ CustomerVariables, ContactListMobileVars }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.dataFeeds.DataFeedsModel
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
 * Created by raghu on 18/12/15.
 */
object CustomerMasterRecordFeed extends DataFeedsModel with Logging {
  val BROWSER_ID = "browserid"
  val ADD4PUSH_ID = "add4push"
  val DEVICE_ID = "device_id"
  override def canProcess(incrDate: String, saveMode: String): Boolean = {
    return true
  }

  override def readDF(incrDate: String, prevDate: String, paths: String): mutable.HashMap[String, DataFrame] = {
    val dfMap: HashMap[String, DataFrame] = new HashMap[String, DataFrame]()
    val dfCmr = CampaignInput.loadCustomerMasterData(incrDate)
    dfMap.put("cmrFull", dfCmr)
    val dfAd4pushId = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.EXTRAS, DataSets.AD4PUSH_ID, DataSets.FULL_MERGE_MODE, incrDate)
    dfMap.put("dfAd4pushId", dfAd4pushId)

    dfMap
  }

  override def write(dfWrite: mutable.HashMap[String, DataFrame], saveMode: String, incrDate: String): Unit = {

    val dfCsv = dfWrite("dfCmrFeed")

    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    DataWriter.writeCsv(dfCsv, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.DAILY_MODE, incrDate, fileDate + "_CID_UID_DID_AD4_ID", DataSets.IGNORE_SAVEMODE, "true", ",", 1)

  }

  override def process(dfMap: mutable.HashMap[String, DataFrame]): mutable.HashMap[String, DataFrame] = {
    val dfCmr = dfMap("cmrFull")
    val dfAd4pushId = dfMap("dfAd4pushId")

    val dfCmrFeed = dfCmr.join(dfAd4pushId, dfCmr(BROWSER_ID) === dfAd4pushId(BROWSER_ID), SQL.LEFT_OUTER)
      .select(
        dfCmr(CustomerVariables.ID_CUSTOMER),
        dfCmr(ContactListMobileVars.UID),
        dfCmr(BROWSER_ID) as DEVICE_ID,
        dfAd4pushId(ADD4PUSH_ID)
      ).na.fill("")

    val dfWrite: HashMap[String, DataFrame] = new HashMap[String, DataFrame]()
    dfWrite.put("dfCmrFeed", dfCmrFeed)

    dfWrite
  }
}
