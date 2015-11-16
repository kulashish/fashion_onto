package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.Daily
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CustomerSelection }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul on 13/11/15.
 */
class LoveBrandCampaign {

  def runCampaign(last35thDaysSalesOrderData: DataFrame, last35thDaySalesOrderItemData: DataFrame, brandMvpRecommendations: DataFrame, yesterdayItrData: DataFrame, incrDate: String = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_TIME_FORMAT)) = {

    val lastOrderCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.LAST_ORDER)

    val dfCustomerSelected = lastOrderCustomerSelector.customerSelection(last35thDaysSalesOrderData, last35thDaySalesOrderItemData)

    val filteredSku = Daily.skuFilter(dfCustomerSelected, yesterdayItrData)

    CampaignUtils.campaignPostProcess(DataSets.CALENDAR_CAMPAIGNS, CampaignCommon.CLEARANCE_CAMPAIGN, filteredSku, false, brandMvpRecommendations)

  }
}
