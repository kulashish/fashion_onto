package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.common.constants.campaign.{ SkuSelection, CustomerSelection, CampaignCommon }

/**
 * Created by raghu on 24/7/15.
 */
class Surf6Campaign {

  def runCampaign(): Unit = {
    val customerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR).getCustomerSelector(CustomerSelection.YESTERDAY_SESSION_DISTINCT)

    val customerSurfData = customerSelector.customerSelection(null)

    val dfSkuSelector = CampaignProducer.getFactory(CampaignCommon.SKU_SELECTOR).getSkuSelector(SkuSelection.SURF)

    //val skuSelector = dfSkuSelector.skuFilter(customerSurfData, null, null)
  }
}
