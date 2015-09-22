package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.FollowUp
import com.jabong.dap.campaign.traceability.PastCampaignCheck
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ SkuSelection, CustomerSelection, CampaignCommon }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul for com.jabong.dap.campaign.campaignlist on 20/7/15.
 */
class AcartFollowUpCampaign {

  def runCampaign(prev3rdDayAcartData: DataFrame, last3DaySalesOrderData: DataFrame, last3DaySalesOrderItemData: DataFrame, itrData: DataFrame, brickMvpRecommendations: DataFrame): Unit = {

    val acartCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.ACART)
    //FIXME:Filter the order items data for last 3 days
    val selectedCustomers = acartCustomerSelector.customerSelection(prev3rdDayAcartData, last3DaySalesOrderData, last3DaySalesOrderItemData)

    //sku selection
    //filter sku based on followup filter
    val filteredSku = FollowUp.skuFilter(selectedCustomers, itrData)

    // ***** mobile push use case
    CampaignUtils.campaignPostProcess(DataSets.PUSH_CAMPAIGNS, CampaignCommon.ACART_FOLLOWUP_CAMPAIGN, filteredSku)

    // ***** email use case
    CampaignUtils.campaignPostProcess(DataSets.EMAIL_CAMPAIGNS, CampaignCommon.ACART_FOLLOWUP_CAMPAIGN, filteredSku, true, brickMvpRecommendations)
  }

}
