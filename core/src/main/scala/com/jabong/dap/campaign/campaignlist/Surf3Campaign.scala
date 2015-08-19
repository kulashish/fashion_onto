package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.Surf
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, SkuSelection }
import com.jabong.dap.common.constants.variables.CustomerPageVisitVariables
import com.jabong.dap.common.time.TimeConstants
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 24/7/15.
 */
class Surf3Campaign {

  def runCampaign(past30DayCampaignMergedData: DataFrame, lastdaySurf3Data: DataFrame, yestItrSkuData: DataFrame, customerMasterData: DataFrame, last30DaySalesOrderData: DataFrame, last30DaySalesOrderItemData: DataFrame): Unit = {

    // rename domain to browserid
    val lastdaySurf3DataFixed = lastdaySurf3Data.withColumnRenamed("device", CustomerPageVisitVariables.BROWER_ID)

    val skus = Surf.skuFilter(past30DayCampaignMergedData, lastdaySurf3DataFixed, yestItrSkuData, customerMasterData, last30DaySalesOrderData, last30DaySalesOrderItemData, CampaignCommon.SURF3_CAMPAIGN)

    val refSkus = CampaignUtils.generateReferenceSkuForSurf(skus, 1)

    val campaignOutput = CampaignUtils.addCampaignMailType(refSkus, CampaignCommon.SURF3_CAMPAIGN)

    //save campaign Output
    CampaignOutput.saveCampaignDataForYesterday(campaignOutput, CampaignCommon.SURF3_CAMPAIGN)

  }
}
