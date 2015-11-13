package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.CategoryReplenishment
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CustomerSelection }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 30/9/15.
 */
class ReplenishmentCampaign {

  def runCampaign(contactListMobileFull: DataFrame, fullSalesOrderData: DataFrame, fullSalesOrderItemData: DataFrame, brickMvpRecommendations: DataFrame, yesterdayItrData: DataFrame) = {

    val customerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.LAST5_SUCCESSFUL_ORDER)

    //    CampaignUtils.debug(contactListMobileFull, "contactListMobileFull")
    //    CampaignUtils.debug(fullSalesOrderData, "fullSalesOrderData")
    //    CampaignUtils.debug(fullSalesOrderItemData, "fullSalesOrderItemData")
    //    CampaignUtils.debug(brickMvpRecommendations, "brickMvpRecommendations")
    //    CampaignUtils.debug(yesterdayItrData, "yesterdayItrData")

    val dfCustomerSelection = customerSelector.customerSelection(contactListMobileFull, fullSalesOrderData, fullSalesOrderItemData)

    //filter sku based on daily filter
    val (dfNonBeautyFrag, dfBeauty) = CategoryReplenishment.skuFilter(dfCustomerSelection, yesterdayItrData)

    CampaignUtils.debug(dfNonBeautyFrag, "dfNonBeautyFrag")
    CampaignUtils.debug(dfBeauty, "dfBeauty")

    // ***** NON_BEAUTY_FRAG_CAMPAIGN email use case
    CampaignUtils.campaignPostProcess(DataSets.CALENDAR_CAMPAIGNS, CampaignCommon.NON_BEAUTY_FRAG_CAMPAIGN, dfNonBeautyFrag, false, brickMvpRecommendations)

    // ***** BEAUTY email use case
    CampaignUtils.campaignPostProcess(DataSets.CALENDAR_CAMPAIGNS, CampaignCommon.BEAUTY_CAMPAIGN, dfBeauty, false, brickMvpRecommendations)

  }

}
