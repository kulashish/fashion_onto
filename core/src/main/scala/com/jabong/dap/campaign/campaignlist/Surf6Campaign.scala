package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.Surf
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ SkuSelection, CustomerSelection, CampaignCommon }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 24/7/15.
 */
class Surf6Campaign {

  def runCampaign(yestSurfSessionData: DataFrame, yestItrSkuData: DataFrame, customerMasterData: DataFrame, yestOrderData: DataFrame, yestOrderItemData: DataFrame, brickMvpRecommendations: DataFrame): Unit = {

    val customerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR).getCustomerSelector(CustomerSelection.YESTERDAY_SESSION_DISTINCT)

    val customerSurfData = customerSelector.customerSelection(yestSurfSessionData)

    val skus = Surf.skuFilter(customerSurfData, yestItrSkuData, customerMasterData, yestOrderData, yestOrderItemData).cache()

    // ***** mobile push use case
    CampaignUtils.campaignPostProcess(DataSets.PUSH_CAMPAIGNS, CampaignCommon.SURF6_CAMPAIGN, skus)

    // ***** email use case
    CampaignUtils.campaignPostProcess(DataSets.EMAIL_CAMPAIGNS, CampaignCommon.SURF6_CAMPAIGN, skus, true, brickMvpRecommendations)

  }

}
