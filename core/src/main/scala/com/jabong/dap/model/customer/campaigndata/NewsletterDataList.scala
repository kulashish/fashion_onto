package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.constants.variables.{ ContactListMobileVars, CustomerVariables, NewsletterVariables }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 14/10/15.
 */
object NewsletterDataList {

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
      col(CustomerVariables.ID_CUSTOMER) as CustomerVariables.CUSTOMER_ID,
      col(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS) as CustomerVariables.EMAIL_SUBSCRIPTION_STATUS,
      col(NewsletterVariables.STATUS) as CustomerVariables.CUR_NL_STATUS
    ).na.fill("")

    dfNlDataList

  }

}
