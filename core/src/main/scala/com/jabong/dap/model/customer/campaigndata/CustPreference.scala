package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ ContactListMobileVars, NewsletterPreferencesVariables, NewsletterVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.customer.variables.NewsletterPreferences
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by mubarak on 4/9/15.
 */
object CustPreference {

  def start(vars: ParamInfo) = {
    val saveMode = vars.saveMode
    val fullPath = OptionUtils.getOptValue(vars.path)
    val incrDate = OptionUtils.getOptValue(vars.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val prevDate = OptionUtils.getOptValue(vars.fullDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER, incrDate))

    val savePath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_PREFERENCE, DataSets.FULL_MERGE_MODE, incrDate)

    if (DataWriter.canWrite(saveMode, savePath)) {

      val (nls, custPrefPrevFull, cmr) = readDf(incrDate, prevDate, fullPath)

      val (nlsInr, custPrefFull) = NewsletterPreferences.getNewsletterPref(nls, custPrefPrevFull)

      custPrefFull.cache()

      //TODO change email with UID
      DataWriter.writeParquet(custPrefFull, savePath, saveMode)

      var res1: DataFrame = custPrefFull
      if (null != custPrefPrevFull) {
        res1 = custPrefFull.except(custPrefPrevFull)
      }

      val res = res1.join(cmr, res1(NewsletterVariables.EMAIL) === cmr(NewsletterVariables.EMAIL), SQL.LEFT_OUTER)
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
      val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
      DataWriter.writeCsv(res, DataSets.VARIABLES, DataSets.CUST_PREFERENCE, DataSets.DAILY_MODE, incrDate, fileDate + "_CUST_PREFERENCE", DataSets.IGNORE_SAVEMODE, "true", ";")
    }
  }

  def readDf(incrDate: String, prevDate: String, fullPath: String): (DataFrame, DataFrame, DataFrame) = {
    var dfNls: DataFrame = null
    var dfcustPrefPrevFull: DataFrame = null
    if (null != fullPath) {
      dfNls = DataReader.getDataFrame4mFullPath(fullPath, DataSets.PARQUET)
    } else {
      dfNls = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.NEWSLETTER_SUBSCRIPTION, DataSets.DAILY_MODE, incrDate)
      dfcustPrefPrevFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_PREFERENCE, DataSets.FULL_MERGE_MODE, prevDate)
    }
    val cmr = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, prevDate)

    (dfNls, dfcustPrefPrevFull, cmr)
  }

}
