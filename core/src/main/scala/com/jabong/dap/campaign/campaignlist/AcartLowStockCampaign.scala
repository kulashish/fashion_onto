package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.LowStock
import com.jabong.dap.campaign.traceability.PastCampaignCheck
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ SkuSelection, CustomerSelection, CampaignCommon }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul for com.jabong.dap.campaign.campaignlist on 20/7/15.
 */
class AcartLowStockCampaign {

  def runCampaign(last30DayAcartData: DataFrame, last30DaySalesOrderData: DataFrame, last30DaySalesOrderItemData: DataFrame, yesterdayItrData: DataFrame, brickMvpRecommendations: DataFrame): Unit = {

    val acartCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.ACART)
    //FIXME:Filter the order items data for last day
    val selectedCustomers = acartCustomerSelector.customerSelection(last30DayAcartData, last30DaySalesOrderData, last30DaySalesOrderItemData)

    CampaignUtils.debug(selectedCustomers,"AcartLowStockCampaigns selected Customer ")
    //sku selection
    //filter sku based on lowstock filter
    val filteredSku = LowStock.skuFilter(selectedCustomers, yesterdayItrData)

    CampaignUtils.debug(filteredSku,"AcartLowStockCampaigns filteredSku ")
    // ***** mobile push use case
    CampaignUtils.campaignPostProcess(DataSets.PUSH_CAMPAIGNS, CampaignCommon.ACART_LOWSTOCK_CAMPAIGN, filteredSku)

    // ***** email use case
    CampaignUtils.campaignPostProcess(DataSets.EMAIL_CAMPAIGNS, CampaignCommon.ACART_LOWSTOCK_CAMPAIGN, filteredSku, true, brickMvpRecommendations)

  }

}
