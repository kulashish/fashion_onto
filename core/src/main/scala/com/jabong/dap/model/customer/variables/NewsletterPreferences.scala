package com.jabong.dap.model.customer.variables

import com.jabong.dap.common.constants.variables.{ NewsletterPreferencesVariables, NewsletterVariables }
import com.jabong.dap.data.storage.merge.common.MergeUtils
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by Kapil.Rajak on 9/7/15.
 */
object NewsletterPreferences extends Logging {

  def getIncrementalNewsletterPref(newsletterSubscription: DataFrame): DataFrame = {
    // :TODO code cleanup
    val dfResult = newsletterSubscription.select(
      col(NewsletterVariables.EMAIL) as NewsletterPreferencesVariables.EMAIL,
      col(NewsletterVariables.NEWSLETTER_PREFERENCES).isNotNull as NewsletterPreferencesVariables.PREF_ALERTS,
      col(NewsletterVariables.NEWSLETTER_PREFERENCES).isNotNull && col(NewsletterVariables.NEWSLETTER_PREFERENCES) contains NewsletterPreferencesVariables.STR_PREF_NL_SALE as NewsletterPreferencesVariables.PREF_NL_SALE,
      col(NewsletterVariables.NEWSLETTER_PREFERENCES).isNotNull && col(NewsletterVariables.NEWSLETTER_PREFERENCES) contains NewsletterPreferencesVariables.STR_PREF_NL_FASHION as NewsletterPreferencesVariables.PREF_NL_FASHION,
      col(NewsletterVariables.NEWSLETTER_PREFERENCES).isNotNull && col(NewsletterVariables.NEWSLETTER_PREFERENCES) contains NewsletterPreferencesVariables.STR_PREF_NL_RECOMMENDATIONS as NewsletterPreferencesVariables.PREF_NL_RECOMENDATIONS,
      col(NewsletterVariables.NEWSLETTER_PREFERENCES).isNotNull && col(NewsletterVariables.NEWSLETTER_PREFERENCES) contains NewsletterPreferencesVariables.STR_PREF_NL_CLEARANCE as NewsletterPreferencesVariables.PREF_NL_CLEARANCE,
      col(NewsletterVariables.NEWSLETTER_PREFERENCES).isNotNull && col(NewsletterVariables.NEWSLETTER_PREFERENCES) contains NewsletterPreferencesVariables.STR_PREF_NL_NEWARRIVALS as NewsletterPreferencesVariables.PREF_NL_NEWARIVALS,
      col(NewsletterVariables.FREQUENCY) as NewsletterPreferencesVariables.PREF_NL_FREQ
    )
    dfResult
  }
  def getMergedNewsletterPref(dfCurr: DataFrame, dfPrev: DataFrame): DataFrame = {
    MergeUtils.InsertUpdateMerge(dfPrev, dfCurr, NewsletterPreferencesVariables.EMAIL)
  }

  def getNewsletterPref(newsletterSubscription: DataFrame, prevCalculated: DataFrame): (DataFrame, DataFrame) = {
    val incrementalResult = getIncrementalNewsletterPref(newsletterSubscription)
    if (null == prevCalculated) {
      return (incrementalResult, incrementalResult)
    }
    val mergedResult = getMergedNewsletterPref(incrementalResult, prevCalculated)
    (incrementalResult, mergedResult)
  }
}
