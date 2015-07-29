package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, SkuSelection }
import com.jabong.dap.common.time.TimeConstants
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 24/7/15.
 */
class Surf3Campaign {

  def runCampaign(surf30DaySessionData: DataFrame, yestItrSkuData: DataFrame, customerMasterData: DataFrame, yestOrderData: DataFrame, yestOrderItemData: DataFrame): Unit = {

    val surfSkuSelector = CampaignProducer.getFactory(CampaignCommon.SKU_SELECTOR).getSkuSelector(SkuSelection.SURF)

    val skus = surfSkuSelector.skuFilter(surf30DaySessionData, yestItrSkuData, customerMasterData, yestOrderData, yestOrderItemData)

    val dfReferenceSku = CampaignUtils.generateReferenceSkuForSurf(skus, 1)

    val campaignOutput = CampaignUtils.addCampaignMailType(dfReferenceSku, CampaignCommon.SURF3_CAMPAIGN)

    //save campaign Output
    CampaignOutput.saveCampaignData(campaignOutput, CampaignCommon.BASE_PATH + "/"
      + CampaignCommon.SURF3_CAMPAIGN + "/" + CampaignUtils.now(TimeConstants.DATE_FORMAT_FOLDER))

  }
}
