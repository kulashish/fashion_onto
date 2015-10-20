package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.constants.variables.{ ContactListMobileVars, CustomerVariables }
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object ContactListPlus extends Logging {

  def getContactListPlus(dfContactListMobileFull: DataFrame, dfContactListMobilePrevFull: DataFrame): DataFrame = {

    val dfContactListPlusFull = dfContactListMobileFull.select(
      col(ContactListMobileVars.UID),
      col(CustomerVariables.PHONE),
      col(CustomerVariables.EMAIL),
      col(ContactListMobileVars.MOBILE_PERMISION_STATUS),
      col(ContactListMobileVars.COUNTRY)
    )

    val dfContactListPlusPrevFull = dfContactListMobilePrevFull.select(
      col(ContactListMobileVars.UID),
      col(CustomerVariables.PHONE),
      col(CustomerVariables.EMAIL),
      col(ContactListMobileVars.MOBILE_PERMISION_STATUS),
      col(ContactListMobileVars.COUNTRY)
    )

    val dfContactListPlus = dfContactListPlusFull.except(dfContactListPlusPrevFull).select(
      col(ContactListMobileVars.UID) as "uid",
      col(CustomerVariables.PHONE) as "mobile",
      col(CustomerVariables.EMAIL) as "email",
      col(ContactListMobileVars.MOBILE_PERMISION_STATUS) as "mobile_permission_status",
      col(ContactListMobileVars.COUNTRY) as "country"
    ).na.fill("")

    dfContactListPlus
  }

}