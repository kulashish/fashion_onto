/*package com.jabong.dap.model.customer.variables

import com.jabong.dap.common.Spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row }
import com.jabong.dap.common.constants.variables.{ PageVisitVariables }

object UserDeviceMapping {

  /**
   * Mapping user to device/browser_id for apps
   * @param dfUser
   * @return (DataFrame)
   */
  def getAppUserDeviceMap (dfUser: DataFrame): DataFrame = {
    if (dfUser == null) {

      log("Data frame should not be null")

      return null

    }

    //Todo schema attributes or data type mismatch


    //val pagevisit = sqlContext.sql("SELECT * from clickstream_apps.apps  WHERE date1=26  AND userid IS NOT NULL AND pagets IS NOT NULL LIMIT 2000")

    val dfAppUser = dfUser.select(

      dfUser.appUserId(col(PageVisitVariables.USER_ID), col(PageVisitVariables.BROWSER_ID)) as PageVisitVariables.USER_ID,

      col(PageVisitVariables.PAGE_TIMESTAMP),
      col(PageVisitVariables.BROWSER_ID),
      col(PageVisitVariables.DOMAIN)
    )
*
    dfAppUser.orderBy(PageVisitVariables.PAGE_TIMESTAMP)
      .groupBy(PageVisitVariables.USER_ID, PageVisitVariables.BROWSER_ID, PageVisitVariables.DOMAIN)
      .agg(PageVisitVariables.USER_ID, PageVisitVariables.BROWSER_ID, PageVisitVariables.DOMAIN, max(PageVisitVariables.PAGE_TIMESTAMP))
      .take(10)

    return dfAppUser
  }
}