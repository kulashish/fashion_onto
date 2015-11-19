package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.Daily
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CustomerSelection }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 14/9/15.
 */
class BrandInCityCampaign {

  def runCampaign(fullCustomerOrders: DataFrame, last6thDaySalesOrderData: DataFrame, last6thDaySalesOrderItemData: DataFrame, brandMvpSubType: DataFrame, yesterdayItrData: DataFrame, incrDate: String) = {

    val customerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.CUSTOMER_PREFERRED_DATA)

    val dfCustomerSelection = customerSelector.customerSelection(fullCustomerOrders, last6thDaySalesOrderData, last6thDaySalesOrderItemData)

    val filteredSku = Daily.skuFilter(dfCustomerSelection, yesterdayItrData)

    // ***** email use case
    CampaignUtils.campaignPostProcess(DataSets.CALENDAR_CAMPAIGNS, CampaignCommon.BRAND_IN_CITY_CAMPAIGN, filteredSku, false, brandMvpSubType)

  }

}
