package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.Daily
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.Utils
import com.jabong.dap.common.constants.campaign.{ CustomerSelection, CampaignCommon }
import com.jabong.dap.common.constants.variables.{ SalesOrderItemVariables, SalesOrderVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul on 10/11/15.
 */
class ClearanceCampaign {

  def runCampaign(fullOrderData: DataFrame, fullSalesOrderItemData: DataFrame, mvpDiscountRecommendations: DataFrame, yesterdayItrData: DataFrame, incrDate: String) = {

    val date = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_FORMAT) + " " + TimeConstants.END_TIME

    val incrDate1 = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_FORMAT)

    CampaignInput.loadNthDayModData(fullOrderData, incrDate1, 10, 30)

    val last10thSalesOrderData = CampaignInput.loadNthDayModData(fullOrderData, incrDate1, 10, 30)
    val last10thSalesOrderItemData = CampaignInput.loadNthDayModData(fullSalesOrderItemData, incrDate1, 10, 30)
    val last30thSalesOrderData = CampaignInput.loadNthDayModData(fullOrderData, incrDate1, 30, 30)
    val last30thSalesOrderItemData = CampaignInput.loadNthDayModData(fullSalesOrderItemData, incrDate1, 30, 30)

    //val last10thSalesOrderData = Utils.getOneDayData(last30DaysSalesOrderData, SalesOrderVariables.CREATED_AT, TimeUtils.getDateAfterNDays(-10, TimeConstants.DATE_TIME_FORMAT, date), TimeConstants.DATE_TIME_FORMAT)
    //val last10thSalesOrderItemData = Utils.getOneDayData(last30DaysSalesOrderItemData, SalesOrderItemVariables.CREATED_AT, TimeUtils.getDateAfterNDays(-10, TimeConstants.DATE_TIME_FORMAT, date), TimeConstants.DATE_TIME_FORMAT)
    //val last30thSalesOrderData = Utils.getOneDayData(last30DaysSalesOrderData, SalesOrderVariables.CREATED_AT, TimeUtils.getDateAfterNDays(-30, TimeConstants.DATE_TIME_FORMAT, date), TimeConstants.DATE_TIME_FORMAT)
    //val last30thSalesOrderItemData = Utils.getOneDayData(last30DaysSalesOrderItemData, SalesOrderItemVariables.CREATED_AT, TimeUtils.getDateAfterNDays(-30, TimeConstants.DATE_TIME_FORMAT, date), TimeConstants.DATE_TIME_FORMAT)

    val salesOrderData = last10thSalesOrderData.unionAll(last30thSalesOrderData)
    val salesOrderItemData = last10thSalesOrderItemData.unionAll(last30thSalesOrderItemData)

    val lastOrderCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.LAST_ORDER)

    val dfCustomerSelected = lastOrderCustomerSelector.customerSelection(salesOrderData, salesOrderItemData)
    CampaignUtils.debug(dfCustomerSelected, CampaignCommon.CLEARANCE_CAMPAIGN + "after customer selection")

    val filteredSku = Daily.skuFilter(dfCustomerSelected, yesterdayItrData)
    CampaignUtils.debug(filteredSku, CampaignCommon.CLEARANCE_CAMPAIGN + "after filteredSku ")

    CampaignUtils.campaignPostProcess(DataSets.CALENDAR_CAMPAIGNS, CampaignCommon.CLEARANCE_CAMPAIGN, filteredSku, false, mvpDiscountRecommendations, incrDate)

  }
}
