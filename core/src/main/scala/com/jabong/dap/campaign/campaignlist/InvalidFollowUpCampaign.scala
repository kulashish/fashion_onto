package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.FollowUp
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{CampaignCommon, CustomerSelection}
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul for invalid follow up campaign campaignlist on 16/7/15.
 */
class InvalidFollowUpCampaign {

  def runCampaign(customerOrderData: DataFrame, orderItemData: DataFrame, itrData: DataFrame,
                  brickMvpRecommendations: DataFrame, incrDate: String) = {

    val invalidCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.INVALID)
    //FIXME:Filter the order items data for 3 days
    val selectedCustomers = invalidCustomerSelector.customerSelection(customerOrderData, orderItemData)

    //sku selection
    //filter sku based on followup filter
    val filteredSku = FollowUp.skuFilter(selectedCustomers, itrData).cache()

    // ***** mobile push use case
    CampaignUtils.campaignPostProcess(DataSets.PUSH_CAMPAIGNS, CampaignCommon.INVALID_FOLLOWUP_CAMPAIGN, filteredSku, true, null, incrDate)

    // ***** email use case
    CampaignUtils.campaignPostProcess(DataSets.EMAIL_CAMPAIGNS, CampaignCommon.INVALID_FOLLOWUP_CAMPAIGN, filteredSku, true, brickMvpRecommendations, incrDate)

  }

}
