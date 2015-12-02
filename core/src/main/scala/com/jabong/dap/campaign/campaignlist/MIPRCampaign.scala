package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.Daily
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CustomerSelection }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 11/9/15.
 */
class MIPRCampaign {

  def runCampaign(last30DaySalesOrderData: DataFrame, yesterdaySalesOrderItemData: DataFrame, brickMvpRecommendations: DataFrame,
                  yesterdayItrData: DataFrame, incrDate: String) = {

    val salesOrderCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.CLOSED_ORDER)

    val dfCustomerSelection = salesOrderCustomerSelector.customerSelection(last30DaySalesOrderData, yesterdaySalesOrderItemData)

    //filter sku based on daily filter
    val filteredSku = Daily.skuFilter(dfCustomerSelection, yesterdayItrData)

    // ***** email use case
    CampaignUtils.campaignPostProcess(DataSets.EMAIL_CAMPAIGNS, CampaignCommon.MIPR_CAMPAIGN, filteredSku, false, brickMvpRecommendations, incrDate)

  }

}