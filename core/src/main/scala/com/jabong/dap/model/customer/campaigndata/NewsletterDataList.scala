package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.constants.variables.{ ContactListMobileVars, CustomerVariables, NewsletterVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 14/10/15.
 */
object NewsletterDataList {

  def writeNLDataList(dfContactListMobileFull: DataFrame, dfContactListMobilePrevFull: DataFrame, incrDate: String) = {

    val dfSelectContactListMobileFull = dfContactListMobileFull.select(
      col(CustomerVariables.EMAIL),
      col(CustomerVariables.ID_CUSTOMER),
      col(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS),
      col(NewsletterVariables.STATUS)
    )

    val dfSelectContactListMobilePrevFull = dfContactListMobilePrevFull.select(
      col(CustomerVariables.EMAIL),
      col(CustomerVariables.ID_CUSTOMER),
      col(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS),
      col(NewsletterVariables.STATUS)
    )

    val dfNlDataList = dfSelectContactListMobileFull.except(dfSelectContactListMobilePrevFull).select(
      col(CustomerVariables.EMAIL),
      col(CustomerVariables.ID_CUSTOMER) as NewsletterVariables.CUSTOMER_ID,
      col(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS) as NewsletterVariables.EMAIL_SUBSCRIPTION_STATUS,
      col(NewsletterVariables.STATUS) as NewsletterVariables.CUR_NL_STATUS
    ).na.fill("")

    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    DataWriter.writeCsv(dfNlDataList, DataSets.VARIABLES, DataSets.NL_DATA_LIST, DataSets.DAILY_MODE, incrDate, "53699_83297_" + fileDate + "_NL_data_list", DataSets.IGNORE_SAVEMODE, "true", ";")

  }

}
