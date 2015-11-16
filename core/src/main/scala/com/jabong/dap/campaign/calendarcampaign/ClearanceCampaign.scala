package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.Daily
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.Utils
import com.jabong.dap.common.constants.campaign.{ CustomerSelection, CampaignCommon }
import com.jabong.dap.common.constants.variables.{ SalesOrderItemVariables, SalesOrderVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.order.variables.SalesOrder
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul on 10/11/15.
 */
class ClearanceCampaign {

  def runCampaign(last30DaysSalesOrderData: DataFrame, last30DaysSalesOrderItemData: DataFrame, mvpDiscountRecommendations: DataFrame, yesterdayItrData: DataFrame, incrDate: String = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_TIME_FORMAT)) = {

    val last10thSalesOrderData = Utils.getOneDayData(last30DaysSalesOrderData, SalesOrderVariables.CREATED_AT, TimeUtils.getDateAfterNDays(-10, TimeConstants.DATE_TIME_FORMAT, incrDate), TimeConstants.DATE_TIME_FORMAT)
    val last10thSalesOrderItemData = Utils.getOneDayData(last30DaysSalesOrderItemData, SalesOrderItemVariables.CREATED_AT, TimeUtils.getDateAfterNDays(-10, TimeConstants.DATE_TIME_FORMAT, incrDate), TimeConstants.DATE_TIME_FORMAT)
    val last30thSalesOrderData = Utils.getOneDayData(last30DaysSalesOrderData, SalesOrderVariables.CREATED_AT, TimeUtils.getDateAfterNDays(-30, TimeConstants.DATE_TIME_FORMAT, incrDate), TimeConstants.DATE_TIME_FORMAT)
    val last30thSalesOrderItemData = Utils.getOneDayData(last30DaysSalesOrderItemData, SalesOrderItemVariables.CREATED_AT, TimeUtils.getDateAfterNDays(-30, TimeConstants.DATE_TIME_FORMAT, incrDate), TimeConstants.DATE_TIME_FORMAT)

    val salesOrderData = last10thSalesOrderData.unionAll(last30thSalesOrderData)
    val salesOrderItemData = last10thSalesOrderItemData.unionAll(last30thSalesOrderItemData)

    val lastOrderCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.LAST_ORDER)

    val dfCustomerSelected = lastOrderCustomerSelector.customerSelection(salesOrderData, salesOrderItemData)

    val filteredSku = Daily.skuFilter(dfCustomerSelected, yesterdayItrData)

    CampaignUtils.campaignPostProcess(DataSets.CALENDAR_CAMPAIGNS, CampaignCommon.CLEARANCE_CAMPAIGN, filteredSku, false, mvpDiscountRecommendations)

  }
}
