package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.Daily
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ CustomerSelection, CampaignCommon }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 29/10/15.
 */
class BrickAffinityCampaign {
  def runCampaign(last7thDaySalesOrderData: DataFrame, last7thDaySalesOrderItemData: DataFrame, brickMvpRecommendations: DataFrame, yesterdayItrData: DataFrame) = {

    val customerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.LAST_ORDER)

    val dfCustomerSelection = customerSelector.customerSelection(last7thDaySalesOrderData, last7thDaySalesOrderItemData)

    //FIXME: call generate ref sku for maximum sku price [email, ref-sku]
    //FIXME: join Customer Favorite data [email, Brick1, Brick2, Brand1, Brand2]
    //FIXME: join Customer Favorite data [email, ref-sku, Brick1, Brick2]
    //FIXME: Generate 8 sku from Brick1 from seller recommendation [email, ref-sku, Brick1 -> [sku1 to sku8]]
    //FIXME: Generate 8 sku from Brick2 from seller recommendation [email, ref-sku, Brick2 -> [sku1 to sku8]]
    //FIXME: join Brick1 to Brick2 [email, ref-sku, Brick1 -> [sku1 to sku8], Brick2 -> [sku1 to sku8]]

    //filter sku based on daily filter
    val filteredSku = Daily.skuFilter(dfCustomerSelection, yesterdayItrData)

    // ***** email use case
    CampaignUtils.campaignPostProcess(DataSets.CALENDAR_CAMPAIGNS, CampaignCommon.BRICK_AFFINITY, filteredSku, false, brickMvpRecommendations)

  }
}
