package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.ItemOnDiscount
import com.jabong.dap.campaign.traceability.PastCampaignCheck
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{SkuSelection, CustomerSelection, CampaignCommon}
import org.apache.spark.sql.DataFrame

/**
 * Created by kapil on 9/9/15.
 */
class InvalidIODCampaign {

  def runCampaign(past30DayCampaignMergedData: DataFrame,customerOrderData: DataFrame, orderItemData: DataFrame,last30daysItrData: DataFrame, brickMvpRecommendations: DataFrame): Unit ={
    val invalidCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.INVALID)
    val selectedCustomers = invalidCustomerSelector.customerSelection(customerOrderData, orderItemData)

    var custFiltered = selectedCustomers

    if (past30DayCampaignMergedData != null) {
      //past campaign check whether the campaign has been sent to customer in last 30 days
      val pastCampaignCheck = new PastCampaignCheck()
      custFiltered = pastCampaignCheck.campaignRefSkuCheck(past30DayCampaignMergedData, selectedCustomers,
        CampaignCommon.campaignMailTypeMap.getOrElse(CampaignCommon.INVALID_LOWSTOCK_CAMPAIGN, 1000), 30)

    }

    //sku selection
    val invalidIod = ItemOnDiscount.skuFilter(custFiltered,last30daysItrData)
    val yesterdayItrData = CampaignUtils.getYesterdayItrData(last30daysItrData)
    val filteredSkuJoinedItr = CampaignUtils.yesterdayItrJoin(invalidIod, yesterdayItrData)
    val refSkus = CampaignUtils.generateReferenceSkus(filteredSkuJoinedItr, CampaignCommon.NUMBER_REF_SKUS)
    val refSkusWithCampaignId = CampaignUtils.addCampaignMailType(refSkus, CampaignCommon.INVALID_IOD_CAMPAIGN)

    // create recommendations
    val recommender = CampaignProducer.getFactory(CampaignCommon.RECOMMENDER).getRecommender(Recommendation.LIVE_COMMON_RECOMMENDER)

    val campaignOutput = recommender.generateRecommendation(refSkusWithCampaignId, brickMvpRecommendations)

    //save campaign Output
    CampaignOutput.saveCampaignDataForYesterday(campaignOutput, CampaignCommon.INVALID_IOD_CAMPAIGN)
  }
}