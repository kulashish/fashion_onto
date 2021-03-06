package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.constants.variables.{ CustomerVariables, ContactListMobileVars }
import com.jabong.dap.common.udf.Udf
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object ContactListPlus extends Logging {

  def getContactListPlus(dfContactListMobileIncr: DataFrame, dfContactListMobilePrevFull: DataFrame): DataFrame = {

    val dfContactListPlusIncr = dfContactListMobileIncr.select(
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

    val dfContactListPlus = dfContactListPlusIncr.except(dfContactListPlusPrevFull).select(
      col(ContactListMobileVars.UID) as CustomerVariables.UID,
      Udf.maskForDecrypt(col(CustomerVariables.PHONE), lit("##")) as CustomerVariables.MOBILE,
      Udf.maskForDecrypt(col(CustomerVariables.EMAIL), lit("**")) as CustomerVariables.EMAIL,
      col(ContactListMobileVars.MOBILE_PERMISION_STATUS) as CustomerVariables.MOBILE_PERMISSION_STATUS,
      col(ContactListMobileVars.COUNTRY) as CustomerVariables.COUNTRY
    ).na.fill("")

    dfContactListPlus
  }

}