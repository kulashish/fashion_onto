package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.Daily
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CustomerSelection }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul for com.jabong.dap.campaign.campaignlist on 23/7/15.
 */
class AcartDailyCampaign {

  def runCampaign(yesterdayAcartData: DataFrame, yesterdaySalesOrderData: DataFrame, yesterdaySalesOrderItemData: DataFrame,
                  yesterdayItrData: DataFrame, brickMvpRecommendations: DataFrame, incrDate: String) = {

    val acartCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.ACART)
    //FIXME:Filter the order items data for last 1 day
    val selectedCustomers = acartCustomerSelector.customerSelection(yesterdayAcartData, yesterdaySalesOrderData, yesterdaySalesOrderItemData)

    CampaignUtils.debug(selectedCustomers, "AcartDaily:-after customer selection")
    //sku selection
    //filter sku based on daily filter
    val filteredSku = Daily.skuFilter(selectedCustomers, yesterdayItrData).cache()

    CampaignUtils.debug(filteredSku, "AcartDaily:-after filteredSku")

    // ***** mobile push use case
    CampaignUtils.campaignPostProcess(DataSets.PUSH_CAMPAIGNS, CampaignCommon.ACART_DAILY_CAMPAIGN, filteredSku, false, null, incrDate)

    // ***** email use case
    CampaignUtils.campaignPostProcess(DataSets.EMAIL_CAMPAIGNS, CampaignCommon.ACART_DAILY_CAMPAIGN, filteredSku, false, brickMvpRecommendations, incrDate)
  }
}

