package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CampaignMergedFields }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul on 13/10/15.
 */
class FollowUpCampaigns {

  def runCampaign(campaignMergedData: DataFrame, salesOrderLast3Days: DataFrame, itrSkuSimpleYesterdayData: DataFrame): Unit = {

    val selectedData = CampaignUtils.campaignSelection(campaignMergedData, salesOrderLast3Days)

    val refSku1Out = CampaignUtils.campaignSkuStockSelection(selectedData, itrSkuSimpleYesterdayData, CampaignMergedFields.LIVE_REF_SKU1, 3)
    val refSku2Out = CampaignUtils.campaignSkuStockSelection(refSku1Out, itrSkuSimpleYesterdayData, CampaignMergedFields.LIVE_REF_SKU + "2", 3)
    val refSku3Out = CampaignUtils.campaignSkuStockSelection(refSku2Out, itrSkuSimpleYesterdayData, CampaignMergedFields.LIVE_REF_SKU + "3", 3)

    CampaignOutput.saveCampaignDataForYesterday(refSku3Out, CampaignCommon.FOLLOW_UP_CAMPAIGNS, DataSets.EMAIL_CAMPAIGNS)
  }
}
