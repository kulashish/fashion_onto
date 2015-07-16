package com.jabong.dap.model.customer.schema

import com.jabong.dap.common.constants.variables.{ NewsletterPreferencesVariables, PaybackCustomerVariables }
import org.apache.spark.sql.types._

/**
 * Created by Kapil.Rajak on 10/7/15.
 */
object NewsletterPrefSchema {

  val newsletterPreferences = StructType(Array(
    StructField(NewsletterPreferencesVariables.EMAIL, StringType, true),
    StructField(NewsletterPreferencesVariables.PREF_NL_SALE, BooleanType, true),
    StructField(NewsletterPreferencesVariables.PREF_NL_FASHION, BooleanType, true),
    StructField(NewsletterPreferencesVariables.PREF_NL_RECOMMENDATIONS, BooleanType, true),
    StructField(NewsletterPreferencesVariables.PREF_ALERTS, BooleanType, true),
    StructField(NewsletterPreferencesVariables.PREF_NL_CLEARANCE, BooleanType, true),
    StructField(NewsletterPreferencesVariables.PREF_NL_NEWARRIVALS, BooleanType, true),
    StructField(NewsletterPreferencesVariables.NEWPREF_NL_FREQ, StringType, true)
  ))
}
