package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.customer.variables.NewsletterPreferences
import com.jabong.dap.common.constants.variables.{ NewsletterPreferencesVariables, NewsletterVariables }
import org.apache.spark.sql.DataFrame

/**
 * Created by mubarak on 4/9/15.
 */
object CustPreference {

  def start(vars: ParamInfo) = {

    val incrDate = OptionUtils.getOptValue(vars.incrDate, TimeUtils.getTodayDate(TimeConstants.DATE_FORMAT_FOLDER))
    val saveMode = vars.saveMode
    val prevDate = OptionUtils.getOptValue(vars.fullDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))


    val (nls, custPref) = readDf(incrDate, prevDate)

    val (nlsInr, custPrefFull) = NewsletterPreferences.getNewsletterPref(nls, custPref)

    val savePath = DataWriter.getWritePath(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_PREFERENCE, DataSets.FULL_MERGE_MODE, incrDate)

    //TODO change email with UID
    DataWriter.writeParquet(custPrefFull, savePath, saveMode)

    val res = custPrefFull.select(
      custPrefFull(NewsletterVariables.EMAIL) as "UID",
      custPrefFull(NewsletterPreferencesVariables.PREF_NL_SALE),
      custPrefFull(NewsletterPreferencesVariables.PREF_NL_FASHION),
      custPrefFull(NewsletterPreferencesVariables.PREF_NL_RECOMMENDATIONS),
      custPrefFull(NewsletterPreferencesVariables.PREF_ALERTS),
      custPrefFull(NewsletterPreferencesVariables.PREF_NL_CLEARANCE),
      custPrefFull(NewsletterPreferencesVariables.PREF_NL_NEWARRIVALS))
    DataWriter.writeCsv(res, ConfigConstants.WRITE_OUTPUT_PATH, DataSets.CUST_PREFERENCE, DataSets.FULL_MERGE_MODE, incrDate, "CUST_PREFERENCE.csv", DataSets.IGNORE_SAVEMODE, "true", ",")

  }

  def readDf(incrDate: String, prevDate: String): (DataFrame, DataFrame) = {

    val dfNls = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_RULE, DataSets.DAILY_MODE, incrDate)

    val dfcustPrefPrevFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_PREFERENCE, DataSets.FULL_MERGE_MODE, prevDate)

    return (dfNls, dfcustPrefPrevFull)
  }

}
