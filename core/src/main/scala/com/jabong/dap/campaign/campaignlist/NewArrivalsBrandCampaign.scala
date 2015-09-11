package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.NewArrivalsBrand
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CustomerSelection }
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 7/9/15.
 */
object NewArrivalsBrandCampaign {
  def runCampaign(salesCart30Days: DataFrame, recommendationsData: DataFrame, yesterdayItrData: DataFrame) = {

    val newArivalsBrandCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.SALES_CART)

    val customerSelected = newArivalsBrandCustomerSelector.customerSelection(salesCart30Days)

    val skus = NewArrivalsBrand.skuFilter(customerSelected, yesterdayItrData)

    //TODO: Fix recommendation Data

    //TODO: generate reference skus
    //    val refSkus = CampaignUtils.generateReferenceSkuForSurf(skus, 1)

  }
}
