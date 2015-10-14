package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ ContactListMobileVars, NewsletterVariables, CustomerVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 14/10/15.
 */
object NewsletterDataList {

  def start(params: ParamInfo) = {

    val incrDate = OptionUtils.getOptValue(params.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))

    val dfIncContactListMobile = DataReader.getDataFrame(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CONTACT_LIST_MOBILE, DataSets.DAILY_MODE, incrDate)

    val dfNlDataList = dfIncContactListMobile.select(
      col(ContactListMobileVars.EMAIL),
      col(CustomerVariables.ID_CUSTOMER) as NewsletterVariables.CUSTOMER_ID,
      col(ContactListMobileVars.EMAIL_SUBSCRIPTION_STATUS) as NewsletterVariables.EMAIL_SUBSCRIPTION_STATUS,
      col(NewsletterVariables.STATUS) as NewsletterVariables.CUR_NL_STATUS
    )

    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    DataWriter.writeCsv(dfNlDataList, DataSets.VARIABLES, DataSets.NEWSLETTER_DATA_LIST, DataSets.DAILY_MODE, incrDate, "53699_83297_" + fileDate + "_NL_data_list", DataSets.IGNORE_SAVEMODE, "true", ";")

  }

}
