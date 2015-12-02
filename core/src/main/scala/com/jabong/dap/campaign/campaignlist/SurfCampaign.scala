package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.data.read.PathBuilder
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.merge.common.DataVerifier
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 28/7/15.
 */
class SurfCampaign extends Logging {

  def runCampaign(
    yestSurfSessionData: DataFrame,
    yestItrSkuData: DataFrame,
    customerMasterData: DataFrame,
    yestOrderData: DataFrame,
    yestOrderItemData: DataFrame,
    lastDaySurf3Data: DataFrame,
    last30DaySalesOrderData: DataFrame,
    last30DaySalesOrderItemData: DataFrame,
    brickMvpRecommendations: DataFrame,
    incrDate: String) = {

    // common customer selection for surf 1, 2, 6

    val surf1Campaign = new Surf1Campaign()
    surf1Campaign.runCampaign(yestSurfSessionData, yestItrSkuData, customerMasterData, yestOrderData, yestOrderItemData, brickMvpRecommendations, incrDate)

    val surf2Campaign = new Surf2Campaign()
    surf2Campaign.runCampaign(yestSurfSessionData, yestItrSkuData, customerMasterData, yestOrderData, yestOrderItemData, brickMvpRecommendations, incrDate)

    val surf6Campaign = new Surf6Campaign()
    surf6Campaign.runCampaign(yestSurfSessionData, yestItrSkuData, customerMasterData, yestOrderData, yestOrderItemData, brickMvpRecommendations, incrDate)

    val surf3DataPath = PathBuilder.buildPath(ConfigConstants.READ_OUTPUT_PATH, DataSets.CLICKSTREAM, "Surf3ProcessedVariable", DataSets.DAILY_MODE, incrDate)

    if (DataVerifier.dataExists(surf3DataPath)) {
      val surf3Campaign = new Surf3Campaign()
      surf3Campaign.runCampaign(lastDaySurf3Data, yestItrSkuData, customerMasterData, last30DaySalesOrderData, last30DaySalesOrderItemData, brickMvpRecommendations, incrDate)
    } else {
      println("Note: Surf3 campaign not run due to surf 3 data not available")
    }

  }

}
