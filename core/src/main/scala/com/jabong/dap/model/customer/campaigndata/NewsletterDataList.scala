package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.constants.variables.{ ContactListMobileVars, CustomerVariables, NewsletterVariables }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 14/10/15.
 */
object NewsletterDataList {

  val CUSTOMER_ID = "customer_id"
  val EMAIL_SUBSCRIPTION_STATUS = "email_subscription_status"
  val CUR_NL_STATUS = "cur_nl_status"

  def getNLDataList(dfContactListMobileIncr: DataFrame, dfContactListMobilePrevFull: DataFrame): DataFrame = {

    val dfNLDataListIncr = dfContactListMobileIncr.select(
      col(CustomerVariables.EMAIL),
      col(CustomerVariables.ID_CUSTOMER),
      col(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS),
      col(NewsletterVariables.STATUS)
    )

    val dfNLDataListPrevFull = dfContactListMobilePrevFull.select(
      col(CustomerVariables.EMAIL),
      col(CustomerVariables.ID_CUSTOMER),
      col(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS),
      col(NewsletterVariables.STATUS)
    )

    val dfNlDataList = dfNLDataListIncr.except(dfNLDataListPrevFull).select(
      col(CustomerVariables.EMAIL),
      col(CustomerVariables.ID_CUSTOMER) as CUSTOMER_ID,
      col(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS) as EMAIL_SUBSCRIPTION_STATUS,
      col(NewsletterVariables.STATUS) as CUR_NL_STATUS
    ).na.fill("")

    dfNlDataList

  }

}
