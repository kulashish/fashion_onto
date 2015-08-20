package com.jabong.dap.model.clickstream.variables

import com.jabong.dap.common.constants.variables.PageVisitVariables
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object UserDeviceMapping {

  /**
   * Mapping user to device/browser_id for apps with preprocessed dataframe
   * @param dfUser
   * @return (DataFrame)
   */
  def getUserDeviceMapApp(dfUser: DataFrame): DataFrame = {
    if (dfUser == null) {

      log("Data frame should not be null")

      return null

    }

    //Todo: schema attributes or data type mismatch

    // Fetch required columns only
    val dfAppUser = dfUser.select(
      col(PageVisitVariables.USER_ID), // Taken care in input DF
      col(PageVisitVariables.BROWSER_ID),
      col(PageVisitVariables.DOMAIN),
      // col(PageVisitVariables.ADD4PUSH),
      col(PageVisitVariables.PAGE_TIMESTAMP)
    )
      //.filter(PageVisitVariables.BROWSER_ID + " IS NOT NULL") Taken care in input DF
      .filter(PageVisitVariables.DOMAIN + " IS NOT NULL")
      .filter(PageVisitVariables.PAGE_TIMESTAMP + " IS NOT NULL")

    // Get user device mapping sorted by time
    val dfuserDeviceMap = dfAppUser
      .filter(PageVisitVariables.USER_ID + " IS NOT NULL")
      .orderBy(PageVisitVariables.PAGE_TIMESTAMP)
      .groupBy(PageVisitVariables.USER_ID, PageVisitVariables.BROWSER_ID, PageVisitVariables.DOMAIN)
      .agg(
        // last(PageVisitVariables.ADD4PUSH) as PageVisitVariables.ADD4PUSH,
        max(PageVisitVariables.PAGE_TIMESTAMP) as PageVisitVariables.PAGE_TIMESTAMP
      )

    return dfuserDeviceMap.select(
      col(PageVisitVariables.USER_ID),
      col(PageVisitVariables.BROWSER_ID),
      col(PageVisitVariables.DOMAIN),
      // col(PageVisitVariables.ADD4PUSH),
      col(PageVisitVariables.PAGE_TIMESTAMP)
    )
  }
}
