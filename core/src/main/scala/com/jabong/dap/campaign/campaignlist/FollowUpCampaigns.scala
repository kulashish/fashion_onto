package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CampaignMergedFields }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul on 13/10/15.
 */
class FollowUpCampaigns {

  /**
   * follow up campaign for surf1,surf2,surf3,surf6 and cancel,return retarget
   * @param campaignMergedData
   * @param salesOrderLast3Days
   * @param itrSkuYesterdayData
   */
  def runCampaign(campaignMergedData: DataFrame, salesOrderLast3Days: DataFrame, itrSkuYesterdayData: DataFrame, incrDate: String) = {

    val selectedData = CampaignUtils.campaignFollowUpSelection(campaignMergedData, salesOrderLast3Days)

    val refSku1Out = CampaignUtils.campaignSkuStockFilter(selectedData, itrSkuYesterdayData, CampaignMergedFields.LIVE_REF_SKU1, 3)
    val refSku2Out = CampaignUtils.campaignSkuStockFilter(refSku1Out, itrSkuYesterdayData, CampaignMergedFields.LIVE_REF_SKU + "2", 3).cache()
    //    val refSku3Out = CampaignUtils.campaignSkuStockFilter(refSku2Out, itrSkuSimpleYesterdayData, CampaignMergedFields.LIVE_REF_SKU + "3", 3)

    val fileName = TimeUtils.getDateAfterNDays(-1, TimeConstants.YYYYMMDD) + "_live_campaign_followup.csv"
    CampaignOutput.saveCampaignData(refSku2Out, CampaignCommon.FOLLOW_UP_CAMPAIGNS, DataSets.EMAIL_CAMPAIGNS, incrDate)

    CampaignOutput.saveCampaignCSVForYesterday(refSku2Out, CampaignCommon.FOLLOW_UP_CAMPAIGNS, fileName, DataSets.EMAIL_CAMPAIGNS)
  }
}