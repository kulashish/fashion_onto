package com.jabong.dap.model.customer.campaigndata

import com.jabong.dap.common.constants.campaign.CampaignMergedFields
import com.jabong.dap.common.constants.variables.{ ContactListMobileVars, CustomerVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
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

  def getAppEmailFeed(dfContactListMobileFull: DataFrame, dfContactListMobilePrevFull: DataFrame): DataFrame = {

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

    val todayDate = TimeUtils.getTodayDate(TimeConstants.DATE_FORMAT)
    val todayStartDate = todayDate + " " + TimeConstants.START_TIME

    val dfAppEmailFeed = dfAppEmailFeedFull.except(dfAppEmailFeedPrevFull).select(
      col(ContactListMobileVars.UID) as UID,
      col(CampaignMergedFields.DEVICE_ID) as DEVICE_ID,
      col(CustomerVariables.EMAIL),
      col(ContactListMobileVars.NET_ORDERS) as NET_ORDERS,
      col(ContactListMobileVars.REG_DATE) as REG_DATE,
      lit(todayStartDate) as PROCESSED_DATE
    ).na.fill("")

    dfAppEmailFeed

  }

}
