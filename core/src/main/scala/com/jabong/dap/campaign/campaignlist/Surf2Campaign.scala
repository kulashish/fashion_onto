package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.Surf
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{CampaignCommon, CustomerSelection}
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 24/7/15.
 */
class Surf2Campaign {

  def runCampaign(yestSurfSessionData: DataFrame, yestItrSkuData: DataFrame, customerMasterData: DataFrame, yestOrderData: DataFrame,
                  yestOrderItemData: DataFrame, brickMvpRecommendations: DataFrame, incrDate: String) = {

    val customerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR).getCustomerSelector(CustomerSelection.YESTERDAY_SESSION)

    val customerSurfData = customerSelector.customerSelection(yestSurfSessionData, yestItrSkuData)

    val skus = Surf.skuFilter(customerSurfData, yestItrSkuData, customerMasterData, yestOrderData, yestOrderItemData).cache()

    // ***** mobile push use case
    CampaignUtils.campaignPostProcess(DataSets.PUSH_CAMPAIGNS, CampaignCommon.SURF2_CAMPAIGN, skus, true, null, incrDate)

    // ***** email use case
    CampaignUtils.campaignPostProcess(DataSets.EMAIL_CAMPAIGNS, CampaignCommon.SURF2_CAMPAIGN, skus, true, brickMvpRecommendations, incrDate)

  }

}
