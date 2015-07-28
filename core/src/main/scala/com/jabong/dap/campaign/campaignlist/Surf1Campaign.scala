package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ SkuSelection, CustomerSelection, CampaignCommon }
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 24/7/15.
 */
class Surf1Campaign {

  def runCampaign(yestSurfSessionData: DataFrame, yestItrSkuData:DataFrame, customerMasterData:DataFrame, yestOrderData:DataFrame, yestOrderItemData:DataFrame): Unit = {
    
    val customerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR).getCustomerSelector(CustomerSelection.YESTERDAY_SESSION)

    val customerSurfData = customerSelector.customerSelection(yestSurfSessionData)

    val surfSkuSelector = CampaignProducer.getFactory(CampaignCommon.SKU_SELECTOR).getSkuSelector(SkuSelection.SURF)

    val skus = surfSkuSelector.skuFilter(customerSurfData, yestItrSkuData, customerMasterData, yestOrderData, yestOrderItemData)

    val dfReferenceSku = CampaignUtils.generateReferenceSku(skus, 1)

    val campaignOutput = CampaignUtils.addCampaignMailType(skus, CampaignCommon.SURF1_CAMPAIGN)

    //save campaign Output
    CampaignOutput.saveCampaignData(campaignOutput, CampaignCommon.BASE_PATH + "/"
      + CampaignCommon.SURF1_CAMPAIGN + "/" + CampaignUtils.now(CampaignCommon.DATE_FORMAT))

  }
}
