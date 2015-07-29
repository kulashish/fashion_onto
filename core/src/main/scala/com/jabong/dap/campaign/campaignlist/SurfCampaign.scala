package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignInput

/**
 * Created by raghu on 28/7/15.
 */
object SurfCampaign {

  def runCampaign(): Unit = {

    // surf 1,2,6
    // one day surf session data
    val yestSurfSessionData = CampaignInput.loadYesterdaySurfSessionData()

    // not bought for last day
    val yestOrderItemData = CampaignInput.loadYesterdayOrderItemData()
    val fullOrderData = CampaignInput.loadFullOrderData()
    val yestOrderData = CampaignInput.loadLastNdaysOrderData(1, fullOrderData)

    val yestItrSkuData = CampaignInput.loadYesterdayItrSkuData()

    // load customer master record for email id to fk_customer mapping
    val customerMasterData = CampaignInput.loadCustomerMasterData()

    // common customer selection for surf 1, 2, 6
    val surf1Campaign = new Surf1Campaign()
    surf1Campaign.runCampaign(yestSurfSessionData, yestItrSkuData, customerMasterData, yestOrderData, yestOrderItemData)

    val surf2Campaign = new Surf2Campaign()
    surf2Campaign.runCampaign(yestSurfSessionData, yestItrSkuData, customerMasterData, yestOrderData, yestOrderItemData)

    val surf6Campaign = new Surf6Campaign()
    surf6Campaign.runCampaign(yestSurfSessionData, yestItrSkuData, customerMasterData, yestOrderData, yestOrderItemData)

    //surf3
    val last30DaySurfSessionData = CampaignInput.loadLast30DaySurfSessionData()

    val surf3Campaign = new Surf3Campaign()
    surf3Campaign.runCampaign(last30DaySurfSessionData, yestItrSkuData, customerMasterData, yestOrderData, yestOrderItemData)

  }

}
