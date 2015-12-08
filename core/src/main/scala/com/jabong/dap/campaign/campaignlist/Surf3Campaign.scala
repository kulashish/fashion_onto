package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.skuselection.Surf
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.variables.PageVisitVariables
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 24/7/15.
 */
class Surf3Campaign {

  def runCampaign(lastdaySurf3Data: DataFrame, yestItrSkuData: DataFrame, customerMasterData: DataFrame, last30DaySalesOrderData: DataFrame,
                  last30DaySalesOrderItemData: DataFrame, brickMvpRecommendations: DataFrame, incrDate: String) = {

    // rename domain to browserid
    val lastdaySurf3DataFixed = lastdaySurf3Data.withColumnRenamed("device", PageVisitVariables.BROWSER_ID)

    val skus = Surf.skuFilter(lastdaySurf3DataFixed, yestItrSkuData, customerMasterData, last30DaySalesOrderData, last30DaySalesOrderItemData).cache()

    // ***** mobile push use case
    CampaignUtils.campaignPostProcess(DataSets.PUSH_CAMPAIGNS, CampaignCommon.SURF3_CAMPAIGN, skus, true, null, incrDate)

    // ***** email use case
    CampaignUtils.campaignPostProcess(DataSets.EMAIL_CAMPAIGNS, CampaignCommon.SURF3_CAMPAIGN, skus, true, brickMvpRecommendations, incrDate)

  }
}
