package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ContactListMobileVars, NewsletterPreferencesVariables, NewsletterVariables}
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.MergeUtils
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.dataFeeds.DataFeedsModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
 * Created by mubarak on 4/9/15.
 */
object CustPreference extends DataFeedsModel {

  override def canProcess(incrDate: String, saveMode: String): Boolean = {
    val savePath = DataWriter.getWritePath (ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_PREFERENCE, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.canWrite(saveMode, savePath)
  }

  override def readDF(incrDate: String, prevDate: String, paths: String): HashMap[String, DataFrame] = {
    val dfMap: mutable.HashMap[String, DataFrame] = new mutable.HashMap[String, DataFrame]()
    var nlsIncr: DataFrame = null
    var custPrefPrevFull: DataFrame = null
    if (null != paths) {
      nlsIncr = DataReader.getDataFrame4mFullPath(paths, DataSets.PARQUET)
    } else {
      nlsIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.NEWSLETTER_SUBSCRIPTION, DataSets.DAILY_MODE, incrDate)
      custPrefPrevFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_PREFERENCE, DataSets.FULL_MERGE_MODE, prevDate)
    }
    val cmrFull = CampaignInput.loadCustomerMasterData(incrDate)
    dfMap.put("nlsIncr", nlsIncr)
    dfMap.put("custPrefPrevFull", custPrefPrevFull)
    dfMap.put("cmrFull", cmrFull)
    dfMap
  }

  override def process(dfMap: mutable.HashMap[String, DataFrame]): mutable.HashMap[String, DataFrame] = {
    val nlsIncr = dfMap("nlsIncr")
    val custPrefPrevFull = dfMap("custPrefPrevFull")

    var custPrefIncr = nlsIncr.select(
      col(NewsletterVariables.EMAIL) as NewsletterPreferencesVariables.EMAIL,
      col(NewsletterVariables.NEWSLETTER_PREFERENCES).isNotNull as NewsletterPreferencesVariables.PREF_ALERTS,
      col(NewsletterVariables.NEWSLETTER_PREFERENCES).isNotNull && col(NewsletterVariables.NEWSLETTER_PREFERENCES) contains NewsletterPreferencesVariables.STR_PREF_NL_SALE as NewsletterPreferencesVariables.PREF_NL_SALE,
      col(NewsletterVariables.NEWSLETTER_PREFERENCES).isNotNull && col(NewsletterVariables.NEWSLETTER_PREFERENCES) contains NewsletterPreferencesVariables.STR_PREF_NL_FASHION as NewsletterPreferencesVariables.PREF_NL_FASHION,
      col(NewsletterVariables.NEWSLETTER_PREFERENCES).isNotNull && col(NewsletterVariables.NEWSLETTER_PREFERENCES) contains NewsletterPreferencesVariables.STR_PREF_NL_RECOMMENDATIONS as NewsletterPreferencesVariables.PREF_NL_RECOMENDATIONS,
      col(NewsletterVariables.NEWSLETTER_PREFERENCES).isNotNull && col(NewsletterVariables.NEWSLETTER_PREFERENCES) contains NewsletterPreferencesVariables.STR_PREF_NL_CLEARANCE as NewsletterPreferencesVariables.PREF_NL_CLEARANCE,
      col(NewsletterVariables.NEWSLETTER_PREFERENCES).isNotNull && col(NewsletterVariables.NEWSLETTER_PREFERENCES) contains NewsletterPreferencesVariables.STR_PREF_NL_NEWARRIVALS as NewsletterPreferencesVariables.PREF_NL_NEWARIVALS,
      col(NewsletterVariables.FREQUENCY) as NewsletterPreferencesVariables.PREF_NL_FREQ)
    var custPrefFull = custPrefIncr
    if (null != custPrefPrevFull) {
      custPrefFull = MergeUtils.InsertUpdateMerge(custPrefPrevFull, custPrefIncr, NewsletterPreferencesVariables.EMAIL)
      custPrefIncr =   custPrefFull.except(custPrefPrevFull)
    }
    val dfWriteMap: mutable.HashMap[String, DataFrame] = new mutable.HashMap[String, DataFrame]()
    dfWriteMap.put("custPrefIncr", custPrefIncr)
    dfWriteMap.put("custPrefFull", custPrefFull)
    dfWriteMap.put("cmrFull", dfMap("cmrFull"))

    dfWriteMap
  }

  override def write(dfWrite: HashMap[String, DataFrame], saveMode: String, incrDate: String) = {
    val cmr = dfWrite("cmrFull")
    val custPrefFull = dfWrite("custPrefFull")
    custPrefFull.cache()
    val custPrefIncr = dfWrite("custPrefIncr")

    val savePath = DataWriter.getWritePath (ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_PREFERENCE, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(custPrefFull, savePath, saveMode)

    val res = custPrefIncr.join(cmr, custPrefIncr(NewsletterVariables.EMAIL) === cmr(NewsletterVariables.EMAIL), SQL.LEFT_OUTER)
      .select(
        cmr(ContactListMobileVars.UID),
        when(custPrefFull(NewsletterPreferencesVariables.PREF_NL_SALE) === true, 1).otherwise(0) as NewsletterPreferencesVariables.PREF_NL_SALE,
        when(custPrefFull(NewsletterPreferencesVariables.PREF_NL_FASHION) === true, 1).otherwise(0) as NewsletterPreferencesVariables.PREF_NL_FASHION,
        when(custPrefFull(NewsletterPreferencesVariables.PREF_NL_RECOMENDATIONS) === true, 1).otherwise(0) as NewsletterPreferencesVariables.PREF_NL_RECOMENDATIONS,
        when(custPrefFull(NewsletterPreferencesVariables.PREF_ALERTS) === true, 1).otherwise(0) as NewsletterPreferencesVariables.PREF_ALERTS,
        when(custPrefFull(NewsletterPreferencesVariables.PREF_NL_CLEARANCE) === true, 1).otherwise(0) as NewsletterPreferencesVariables.PREF_NL_CLEARANCE,
        when(custPrefFull(NewsletterPreferencesVariables.PREF_NL_NEWARIVALS) === true, 1).otherwise(0) as NewsletterPreferencesVariables.PREF_NL_NEWARIVALS,
        custPrefFull(NewsletterPreferencesVariables.PREF_NL_FREQ))
      .na.fill("")

    val savePathIncr = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_PREFERENCE, DataSets.DAILY_MODE, incrDate)
    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    DataWriter.writeParquet(res, savePathIncr, saveMode)
    DataWriter.writeCsv(res, DataSets.VARIABLES, DataSets.CUST_PREFERENCE, DataSets.DAILY_MODE, incrDate, fileDate + "_CUST_PREFERENCE", DataSets.IGNORE_SAVEMODE, "true", ";", 1)
  }
}
