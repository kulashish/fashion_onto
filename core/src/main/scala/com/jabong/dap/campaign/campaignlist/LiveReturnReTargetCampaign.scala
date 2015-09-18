package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.ReturnReTarget
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ Recommendation, SkuSelection, CampaignCommon }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul for return reTarget campaign on 7/7/15.
 */
class LiveReturnReTargetCampaign {

  def runCampaign(targetCustomersWithOrderItems: DataFrame, yesterdayItrData: DataFrame, brickMvpRecommendations: DataFrame): Unit = {

    // filter only by return status
    // filter by campaign -- i.e., campaign should not have gone to customer in last 30 days

    // find list of (customers, ref skus)
    val filteredSkus = ReturnReTarget.skuFilter(targetCustomersWithOrderItems)

    // save 2 ref skus + 8 recommendation per customer (null allowed for mobile push)
    val filteredSkuJoinedItr = CampaignUtils.yesterdayItrJoin(filteredSkus, yesterdayItrData)


    // ***** mobile push use case
    CampaignUtils.campaignPostProcess(DataSets.PUSH_CAMPAIGNS, CampaignCommon.RETURN_RETARGET_CAMPAIGN, filteredSkuJoinedItr)

    // ***** email use case
    CampaignUtils.campaignPostProcess(DataSets.EMAIL_CAMPAIGNS, CampaignCommon.RETURN_RETARGET_CAMPAIGN, filteredSkuJoinedItr)

  }
}
