package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.MostBought
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{CustomerSelection, CampaignCommon}
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul on 18/11/15.
 */
class LoveColorCampaign {

  def runCampaign(customerTopData:DataFrame,last15thDaysSalesOrderData: DataFrame, last15thDaySalesOrderItemData: DataFrame, mvpColorRecommendation: DataFrame, yesterdayItrData: DataFrame, incrDate: String = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_TIME_FORMAT)) = {

    val lastOrderCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.LAST_ORDER)

    val dfLastOrderCustomerSelected = lastOrderCustomerSelector.customerSelection(last15thDaysSalesOrderData, last15thDaySalesOrderItemData)

    CampaignUtils.debug(dfLastOrderCustomerSelected, CampaignCommon.LOVE_COLOR_CAMPAIGN+"after customer selection")

    val filteredSku = MostBought.skuFilter(customerTopData,dfLastOrderCustomerSelected, yesterdayItrData,"color_list")

    CampaignUtils.debug(filteredSku, CampaignCommon.LOVE_COLOR_CAMPAIGN+"after filteredSku ")

    CampaignUtils.campaignPostProcess(DataSets.CALENDAR_CAMPAIGNS, CampaignCommon.LOVE_COLOR_CAMPAIGN, filteredSku, false, mvpColorRecommendation)

  }
}
