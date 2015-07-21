package com.jabong.dap.model.clickstream.variables

import com.jabong.dap.common.Spark
import org.apache.spark.sql.functions._
import com.jabong.dap.common.udf.{ Udf, UdfUtils }
import org.apache.spark.sql.{ DataFrame, Row }
import com.jabong.dap.common.constants.variables.{ PageVisitVariables }

object UserDeviceMapping {

  /**
   * Mapping user to device/browser_id for apps
   * @param dfUser
   * @return (DataFrame)
   */
  def getUserDeviceMap (dfUser: DataFrame): DataFrame = {
    if (dfUser == null) {

      log("Data frame should not be null")

      return null

    }

    //Todo: schema attributes or data type mismatch

    // Fetch required columns only and change null userid for apps to _app_{browser_id}
    val dfAppUser = dfUser.select(

      Udf.appUserId(
        col(PageVisitVariables.USER_ID),
        col(PageVisitVariables.DOMAIN),
        col(PageVisitVariables.BROWSER_ID)
      ) as PageVisitVariables.USER_ID,

      col(PageVisitVariables.BROWSER_ID),
      col(PageVisitVariables.DOMAIN),
      col(PageVisitVariables.PAGE_TIMESTAMP)
    )
     .filter(PageVisitVariables.BROWSER_ID + " IS NOT NULL")
      .filter(PageVisitVariables.DOMAIN + " IS NOT NULL")
      .filter(PageVisitVariables.PAGE_TIMESTAMP + " IS NOT NULL")

    // Get user device mapping sorted by time
    val dfuserDeviceMap = dfAppUser.filter(PageVisitVariables.USER_ID + " IS NOT NULL")
      .orderBy(PageVisitVariables.PAGE_TIMESTAMP)
      .groupBy(PageVisitVariables.USER_ID, PageVisitVariables.BROWSER_ID, PageVisitVariables.DOMAIN)
      .agg(
        max(PageVisitVariables.PAGE_TIMESTAMP) as PageVisitVariables.PAGE_TIMESTAMP
      )

    return dfuserDeviceMap
  }
}
