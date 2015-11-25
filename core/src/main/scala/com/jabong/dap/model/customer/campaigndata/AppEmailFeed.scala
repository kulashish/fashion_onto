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

  def getAppEmailFeed(dfContactListMobileIncr: DataFrame, dfContactListMobilePrevFull: DataFrame): DataFrame = {

    val dfAppEmailFeedIncr = dfContactListMobileIncr.select(
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

    val todayStartDate = TimeUtils.getTodayDate(TimeConstants.DATE_FORMAT) + " " + TimeConstants.START_TIME

    val dfAppEmailFeed = dfAppEmailFeedIncr.except(dfAppEmailFeedPrevFull).select(
      col(ContactListMobileVars.UID) as CustomerVariables.UID,
      col(CampaignMergedFields.DEVICE_ID) as CampaignMergedFields.deviceId,
      col(CustomerVariables.EMAIL),
      col(ContactListMobileVars.NET_ORDERS) as CustomerVariables.NET_ORDERS,
      col(ContactListMobileVars.REG_DATE) as CustomerVariables.REG_DATE,
      lit(todayStartDate) as CustomerVariables.PROCESSED_DATE
    ).na.fill(Map(
      CustomerVariables.UID -> "",
      CampaignMergedFields.deviceId -> "",
      CustomerVariables.EMAIL -> "",
      CustomerVariables.NET_ORDERS -> "",
      CustomerVariables.REG_DATE -> "",
      CustomerVariables.PROCESSED_DATE -> ""
    ))

    dfAppEmailFeed

  }

}
