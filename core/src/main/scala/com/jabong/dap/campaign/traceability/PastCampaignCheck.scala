package com.jabong.dap.campaign.traceability

import com.jabong.dap.common.constants.campaign.CampaignMerge
import com.jabong.dap.common.constants.variables.{ ProductVariables, CustomerVariables }
import com.jabong.dap.common.time.{ Constants, TimeUtils }
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * To check whether we send the campaign to the user earlier
 */
class PastCampaignCheck extends Logging {

  /**
   *
   * @param pastCampaignData
   * @param campaignMailType
   * @param nDays
   * @return
   */
  def getCampaignCustomers(pastCampaignData: DataFrame, campaignMailType: Int, nDays: Int): DataFrame = {
    if (pastCampaignData == null || campaignMailType == 0 || nDays < 0) {
      logger.error("Any of the argument is null")
      return null
    }
    //FIXME: Add campaign type check in HashMap

    val filterDate = TimeUtils.getDateAfterNDays(-nDays, Constants.DATE_FORMAT)

    val mailTypeCustomers = pastCampaignData.filter(CampaignMerge.CAMPAIGN_MAIL_TYPE + " = " + campaignMailType + " and " + CampaignMerge.END_OF_DATE + " >= '" + filterDate + "'")
      .select(pastCampaignData(CampaignMerge.FK_CUSTOMER) as CustomerVariables.FK_CUSTOMER, pastCampaignData(CampaignMerge.REF_SKU1) as ProductVariables.SKU)
    logger.info("Filtering campaign customer based on mail type" + campaignMailType + " and date >= " + filterDate)

    return mailTypeCustomers
  }
}
