package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.CancelReTarget
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ Recommendation, CampaignCommon, SkuSelection }
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

    val refSkus = CampaignUtils.generateReferenceSkus(filteredSkuJoinedItr, CampaignCommon.NUMBER_REF_SKUS)

    val refSkusWithCampaignId = CampaignUtils.addCampaignMailType(refSkus, CampaignCommon.CANCEL_RETARGET_CAMPAIGN)

    // create recommendations
    val recommender = CampaignProducer.getFactory(CampaignCommon.RECOMMENDER).getRecommender(Recommendation.LIVE_COMMON_RECOMMENDER)

    val campaignOutput = recommender.generateRecommendation(refSkusWithCampaignId, brickMvpRecommendations)

    //save campaign Output
    CampaignOutput.saveCampaignDataForYesterday(campaignOutput, CampaignCommon.CANCEL_RETARGET_CAMPAIGN)

  }

}
