package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.{MostBought, Daily}
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CustomerSelection }
import com.jabong.dap.common.constants.variables.{ProductVariables, CustomerVariables}
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
 * Created by rahul on 13/11/15.
 */
class LoveBrandCampaign {

  def runCampaign(customerTopData:DataFrame,last35thDaysSalesOrderData: DataFrame, last35thDaySalesOrderItemData: DataFrame, brandMvpRecommendations: DataFrame, yesterdayItrData: DataFrame, incrDate: String = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_TIME_FORMAT)) = {

    val lastOrderCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.LAST_ORDER)

    val dfCustomerSelected = lastOrderCustomerSelector.customerSelection(last35thDaysSalesOrderData, last35thDaySalesOrderItemData)

    val filteredSku = MostBought.skuFilter(customerTopData,dfCustomerSelected, yesterdayItrData,"brand_list")

    CampaignUtils.campaignPostProcess(DataSets.CALENDAR_CAMPAIGNS, CampaignCommon.LOVE_BRAND_CAMPAIGN, filteredSku, false, brandMvpRecommendations)

  }
}
