package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.constants.variables.{ContactListMobileVars, CustomerVariables, NewsletterVariables}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 14/10/15.
 */
object NewsletterDataList {

  def getNLDataList(dfContactListMobileFull: DataFrame, dfContactListMobilePrevFull: DataFrame) : DataFrame = {

    val dfNLDataListFull = dfContactListMobileFull.select(
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

    val dfNlDataList = dfNLDataListFull.except(dfNLDataListPrevFull).select(
      col(CustomerVariables.EMAIL),
      col(CustomerVariables.ID_CUSTOMER) as NewsletterVariables.CUSTOMER_ID,
      col(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS) as NewsletterVariables.EMAIL_SUBSCRIPTION_STATUS,
      col(NewsletterVariables.STATUS) as NewsletterVariables.CUR_NL_STATUS
    ).na.fill("")

    dfNlDataList

  }

}
