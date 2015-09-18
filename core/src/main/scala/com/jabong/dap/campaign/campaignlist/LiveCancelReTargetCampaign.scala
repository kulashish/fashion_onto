package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.CancelReTarget
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ Recommendation, CampaignCommon, SkuSelection }
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.data.read.PathBuilder
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul for live cancel reTarget campaign on 7/7/15.
 */
class LiveCancelReTargetCampaign {

  def runCampaign(targetCustomersWithOrderItems: DataFrame, yesterdayItrData: DataFrame, brickMvpRecommendations: DataFrame): Unit = {
    // targetCustomersWithOrderItems = (id_customer, id_sales_order, item_status, unit_price, updated_at, sku_simple)

    // filter only by return status
    // FIXME: filter by campaign -- i.e., campaign shount not have gone to customer in last 30 days
    // find list of (customers, ref skus)
    val filteredSkus = CancelReTarget.skuFilter(targetCustomersWithOrderItems)

    // save 2 ref skus + 8 recommendation per customer (null allowed for mobile push)
    val filteredSkuJoinedItr = CampaignUtils.yesterdayItrJoin(filteredSkus, yesterdayItrData)

    // ***** mobile push use case
    CampaignUtils.campaignPostProcess(DataSets.PUSH_CAMPAIGNS, CampaignCommon.CANCEL_RETARGET_CAMPAIGN, filteredSkuJoinedItr, false)

    // ***** email use case
    CampaignUtils.campaignPostProcess(DataSets.EMAIL_CAMPAIGNS, CampaignCommon.CANCEL_RETARGET_CAMPAIGN, filteredSkuJoinedItr, false, brickMvpRecommendations)


  }

}
