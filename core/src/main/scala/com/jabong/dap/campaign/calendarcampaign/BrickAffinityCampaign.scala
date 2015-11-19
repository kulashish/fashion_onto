package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.Daily
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.{ Utils, Spark }
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.{ CampaignMergedFields, CustomerSelection, CampaignCommon }
import com.jabong.dap.common.constants.variables.{ ProductVariables, CustomerVariables }
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.{ Row, DataFrame }

/**
 * Created by raghu on 29/10/15.
 */
class BrickAffinityCampaign {

  val BRICK1 = "BRICK1"
  val BRICK2 = "BRICK2"
  def runCampaign(customerSurfAffinity: DataFrame, last7thDaySalesOrderData: DataFrame, last7thDaySalesOrderItemData: DataFrame, brickMvpRecommendations: DataFrame, yesterdayItrData: DataFrame, incrDate: String) = {

    val customerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.LAST_ORDER)

    val dfCustomerSelection = customerSelector.customerSelection(last7thDaySalesOrderData, last7thDaySalesOrderItemData)
    CampaignUtils.debug(dfCustomerSelection, "dfCustomerSelection")
    //filter sku based on daily filter
    val filteredSku = Daily.skuFilter(dfCustomerSelection, yesterdayItrData)
    CampaignUtils.debug(filteredSku, "filteredSku")

    //join Customer Favorite data [email, Brick1, Brick2]
    val customerFavBrick = Utils.getCustomerFavBrick(customerSurfAffinity)
    CampaignUtils.debug(customerFavBrick, "customerFavBrick")

    //join Customer Favorite data [email, ref-sku, Brick1, Brick2]
    val joinedToFavBrick = filteredSku.join(customerFavBrick, filteredSku(CustomerVariables.EMAIL) === customerFavBrick(CustomerVariables.EMAIL), SQL.INNER)
      .select(
        filteredSku("*"),
        customerFavBrick(BRICK1),
        customerFavBrick(BRICK2)
      )
    CampaignUtils.debug(joinedToFavBrick, "joinedToFavBrick")

    //Generate 8 sku from Brick1 from seller recommendation [email, ref-sku, Brick1 -> [sku1 to sku8]]
    //Generate 8 sku from Brick2 from seller recommendation [email, ref-sku, Brick2 -> [sku1 to sku8]]
    //join Brick1 to Brick2 [email, ref-sku, Brick1 -> [sku1 to sku8], Brick2 -> [sku1 to sku8]]

    // ***** email use case
    CampaignUtils.campaignPostProcess(DataSets.CALENDAR_CAMPAIGNS, CampaignCommon.BRICK_AFFINITY_CAMPAIGN, joinedToFavBrick, false, brickMvpRecommendations)

  }
}
