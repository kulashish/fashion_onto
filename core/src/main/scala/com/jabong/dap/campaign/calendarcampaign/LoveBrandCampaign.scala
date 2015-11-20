package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.{ MostBought, Daily }
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CustomerSelection }
import com.jabong.dap.common.constants.variables.{ ProductVariables, CustomerVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by rahul on 13/11/15.
 */
class LoveBrandCampaign {

  /**
   * love brand campaign:- last sku of most bought brand
   * @param customerTopData
   * @param last35thDaysSalesOrderData
   * @param last35thDaySalesOrderItemData
   * @param brandMvpRecommendations
   * @param yesterdayItrSkuSimpleData
   * @param incrDate
   */
  def runCampaign(customerTopData: DataFrame, last35thDaysSalesOrderData: DataFrame, last35thDaySalesOrderItemData: DataFrame, brandMvpRecommendations: DataFrame, yesterdayItrSkuSimpleData: DataFrame, incrDate: String = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_TIME_FORMAT)) = {

    val lastOrderCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.LAST_ORDER)

    val dfLastOrderCustomerSelected = lastOrderCustomerSelector.customerSelection(last35thDaysSalesOrderData, last35thDaySalesOrderItemData)

    CampaignUtils.debug(dfLastOrderCustomerSelected, CampaignCommon.LOVE_BRAND_CAMPAIGN + "after customer selection")

    val filteredSku = MostBought.skuFilter(customerTopData, dfLastOrderCustomerSelected, yesterdayItrSkuSimpleData, "brand_list")

    CampaignUtils.debug(filteredSku, CampaignCommon.LOVE_BRAND_CAMPAIGN + "after filteredSku ")

    CampaignUtils.campaignPostProcess(DataSets.CALENDAR_CAMPAIGNS, CampaignCommon.LOVE_BRAND_CAMPAIGN, filteredSku, false, brandMvpRecommendations)

  }
}
