package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ SkuSelection, CustomerSelection, CampaignCommon }
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 24/7/15.
 */
class Surf2Campaign {

  def runCampaign(yestSurfSessionData: DataFrame, yestItrSkuData: DataFrame, customerMasterData: DataFrame, yestOrderData: DataFrame, yestOrderItemData: DataFrame): Unit = {

    val customerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR).getCustomerSelector(CustomerSelection.YESTERDAY_SESSION)

    val customerSurfData = customerSelector.customerSelection(yestSurfSessionData, yestItrSkuData)

    val surfSkuSelector = CampaignProducer.getFactory(CampaignCommon.SKU_SELECTOR).getSkuSelector(SkuSelection.SURF)

    val skus = surfSkuSelector.skuFilter(customerSurfData, yestItrSkuData, customerMasterData, yestOrderData, yestOrderItemData)

    val dfReferenceSku = CampaignUtils.generateReferenceSkuForSurf(skus, 1)

    val campaignOutput = CampaignUtils.addCampaignMailType(dfReferenceSku, CampaignCommon.SURF2_CAMPAIGN)

    //save campaign Output
    CampaignOutput.saveCampaignData(campaignOutput, CampaignCommon.BASE_PATH + "/"
      + CampaignCommon.SURF2_CAMPAIGN + "/" + CampaignUtils.now(CampaignCommon.DATE_FORMAT))

  }

}
