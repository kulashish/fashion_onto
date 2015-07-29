package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.data.read.PathBuilder
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.DataVerifier

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
    val fullOrderItemData = CampaignInput.loadFullOrderItemData()

    val last30DaySalesOrderItemData = CampaignInput.loadLastNdaysOrderItemData(30, fullOrderItemData) // created_at
    val last30DaySalesOrderData = CampaignInput.loadLastNdaysOrderData(30, fullOrderData)

    val dateYesterday = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER)
    val surf3DataPath = PathBuilder.buildPath(DataSets.OUTPUT_PATH, DataSets.CLICKSTREAM, "Surf3ProcessedVariable", DataSets.DAILY_MODE, dateYesterday)
    if (DataVerifier.dataExists(surf3DataPath)) {
      val lastDaySurf3Data = CampaignInput.loadLastDaySurf3Data()
      val surf3Campaign = new Surf3Campaign()
      surf3Campaign.runCampaign(lastDaySurf3Data, yestItrSkuData, customerMasterData, last30DaySalesOrderData, last30DaySalesOrderItemData)
    } else {
      println("Note: Surf3 campaign not run due to surf 3 data not available")
    }

  }

}
