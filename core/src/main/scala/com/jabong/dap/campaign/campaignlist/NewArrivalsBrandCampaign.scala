package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.NewArrivalsBrand
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ SkuSelection, CustomerSelection, CampaignCommon }
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 7/9/15.
 */
class NewArrivalsBrandCampaign {
  def runCampaign(salesCart30Days: DataFrame, recommendationsData: DataFrame, yesterdayItrData: DataFrame) = {

    val newArivalsBrandCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.NEW_ARRIVALS_BRAND)

    val customerSelected = newArivalsBrandCustomerSelector.customerSelection(salesCart30Days)

    val skus = NewArrivalsBrand.skuFilter(customerSelected, yesterdayItrData)

    //TODO: generate reference skus
    //    val refSkus = CampaignUtils.generateReferenceSkuForSurf(skus, 1)

  }
}
