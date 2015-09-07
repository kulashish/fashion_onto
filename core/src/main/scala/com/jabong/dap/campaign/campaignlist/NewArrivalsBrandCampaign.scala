package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.NewArrivalsBrand
import com.jabong.dap.common.constants.campaign.{ SkuSelection, CustomerSelection, CampaignCommon }

/**
 * Created by raghu on 7/9/15.
 */
class NewArrivalsBrandCampaign {
  def runCampaign(): Unit = {

    val salesCart30Day = CampaignInput.loadLast30daysAcartData()

    val newArivalsBrandCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.NEW_ARRIVALS_BRAND)

    val customerSelected = newArivalsBrandCustomerSelector.customerSelection(salesCart30Day)

    //TODO: make itr data for NewArrivalsBrand
    val itrData = CampaignInput.loadYesterdayItrSkuData()

    val skus = NewArrivalsBrand.skuFilter(itrData)

  }
}
