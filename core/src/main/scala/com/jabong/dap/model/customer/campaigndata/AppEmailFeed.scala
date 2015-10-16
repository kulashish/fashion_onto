package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.constants.campaign.CampaignMergedFields
import com.jabong.dap.common.constants.variables.{ PageVisitVariables, ContactListMobileVars, NewsletterVariables, CustomerVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 15/10/15.
 */
object AppEmailFeed {

  val PROCESSED_DATE = "processed_date"
  val NET_ORDERS = "net_orders"
  val REG_DATE = "reg_date"
  val UID = "uid"
  val DEVICE_ID = "deviceid"

  def writeAppEmailFeed(dfContactListMobileFull: DataFrame, dfContactListMobilePrevFull: DataFrame, incrDate: String) = {

    val dfAppEmailFeedFull = dfContactListMobileFull.select(
      col(ContactListMobileVars.UID),
      col(CampaignMergedFields.DEVICE_ID),
      col(CustomerVariables.EMAIL),
      col(ContactListMobileVars.NET_ORDERS),
      col(ContactListMobileVars.REG_DATE)
    )

    val dfAppEmailFeedPrevFull = dfContactListMobilePrevFull.select(
      col(ContactListMobileVars.UID),
      col(CampaignMergedFields.DEVICE_ID),
      col(CustomerVariables.EMAIL),
      col(ContactListMobileVars.NET_ORDERS),
      col(ContactListMobileVars.REG_DATE)
    )

    val todayDate = TimeUtils.getTodayDate(TimeConstants.DATE_TIME_FORMAT)
    val todayStartDate = todayDate.substring(0, todayDate.indexOf(" ") + 1) + TimeConstants.START_TIME

    val dfAppEmailFeed = dfAppEmailFeedFull.except(dfAppEmailFeedPrevFull).select(
      col(ContactListMobileVars.UID) as UID,
      col(CampaignMergedFields.DEVICE_ID) as DEVICE_ID,
      col(CustomerVariables.EMAIL),
      col(ContactListMobileVars.NET_ORDERS) as NET_ORDERS,
      col(ContactListMobileVars.REG_DATE) as REG_DATE
    ).withColumn(PROCESSED_DATE, lit(todayStartDate)).na.fill("")

    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    DataWriter.writeCsv(dfAppEmailFeed, DataSets.VARIABLES, DataSets.APP_EMAIL_FEED, DataSets.DAILY_MODE, incrDate, "53699_80036_" + fileDate + "_app_email_feed", DataSets.IGNORE_SAVEMODE, "true", ";")

  }

}
