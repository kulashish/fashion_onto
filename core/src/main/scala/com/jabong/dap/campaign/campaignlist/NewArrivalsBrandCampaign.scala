package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.NewArrivalsBrand
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CustomerSelection }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 7/9/15.
 */
class NewArrivalsBrandCampaign {
  def runCampaign(last30DaySalesOrderData: DataFrame, last30DaySalesOrderItemData: DataFrame, salesCart30Days: DataFrame, brandMvpRecommendations: DataFrame, yesterdayItrData: DataFrame) = {

    val newArivalsBrandCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.SALES_CART)

    val customerSelected = newArivalsBrandCustomerSelector.customerSelection(last30DaySalesOrderData, last30DaySalesOrderItemData, salesCart30Days)

    val filteredSku = NewArrivalsBrand.skuFilter(customerSelected, yesterdayItrData)

    // ***** email use case
    CampaignUtils.campaignPostProcess(DataSets.EMAIL_CAMPAIGNS, CampaignCommon.NEW_ARRIVALS_BRAND, filteredSku, true, brandMvpRecommendations)

  }
}
