package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.{ItemOnDiscount, Daily}
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{CampaignCommon, CustomerSelection}
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by kapil on 9/9/15.
 */
class InvalidIODCampaign {

  def runCampaign(customerOrderData: DataFrame, orderItemData: DataFrame, last30DaysItrData: DataFrame, brickMvpRecommendations: DataFrame): Unit = {
    val invalidCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.INVALID)
    val selectedCustomers = invalidCustomerSelector.customerSelection(customerOrderData, orderItemData)
    val filteredSku = ItemOnDiscount.skuFilter(selectedCustomers, last30DaysItrData)

    CampaignUtils.campaignPostProcess(DataSets.EMAIL_CAMPAIGNS, CampaignCommon.INVALID_IOD_CAMPAIGN, filteredSku, false, brickMvpRecommendations)
  }
}