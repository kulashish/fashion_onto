package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.Daily
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CustomerSelection }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 16/9/15.
 */
class PricepointCampaign {

  def runCampaign(last20thDaySalesOrderData: DataFrame, last20thDaySalesOrderItemData: DataFrame, brickMvpRecommendations: DataFrame, yesterdayItrData: DataFrame) = {

    val customerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.LAST_ORDER)

    val dfCustomerSelection = customerSelector.customerSelection(last20thDaySalesOrderData, last20thDaySalesOrderItemData)

    //filter sku based on daily filter
    val filteredSku = Daily.skuFilter(dfCustomerSelection, yesterdayItrData)

    // ***** email use case
    CampaignUtils.campaignPostProcess(DataSets.CALENDAR_CAMPAIGNS, CampaignCommon.PRICEPOINT_CAMPAIGN, filteredSku, false, brickMvpRecommendations)

  }

}
