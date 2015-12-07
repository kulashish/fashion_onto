package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.Daily
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.Utils
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CustomerSelection }
import com.jabong.dap.common.constants.variables.SalesOrderVariables
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by samathashetty on 9/11/15.
 */
class HottestXCampaign {

  def runCampaign(fullOrderData: DataFrame, fullOrderItemData: DataFrame, yesterdayItr: DataFrame, recommendations: DataFrame, incrDate: String) {
    val incrDate1 = TimeUtils.changeDateFormat(incrDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_FORMAT)

    val last60DaySalesOrderData: DataFrame = CampaignInput.loadNthDayModData(fullOrderData, incrDate1, 60, 60).filter(last60DaySalesOrderData(SalesOrderVariables.GRAND_TOTAL).>(1000))
    val last60DaySalesOrderItemData: DataFrame =  CampaignInput.loadNthDayModData(fullOrderItemData, incrDate1, 60, 60)

    val last45DaySalesOrderData: DataFrame = CampaignInput.loadNthDayModData(fullOrderData, incrDate1, 60, 60).filter(last45DaySalesOrderData(SalesOrderVariables.GRAND_TOTAL).<=(1000))
    val last45DaySalesOrderItemData: DataFrame =  CampaignInput.loadNthDayModData(fullOrderItemData, incrDate1, 60, 60)
    
/*    
    val day_45past = TimeUtils.getDateAfterNDays(-45, TimeConstants.DATE_FORMAT_FOLDER)

    val days_45_filter = Utils.getOneDayData(nDaysSalesOrder, SalesOrderVariables.CREATED_AT, day_45past, TimeConstants.DATE_FORMAT_FOLDER)
      .filter(nDaysSalesOrder(SalesOrderVariables.GRAND_TOTAL).<=(1000))

    val day_60past = TimeUtils.getDateAfterNDays(-60, TimeConstants.DATE_FORMAT_FOLDER)

    val days_60_filter = Utils.getOneDayData(nDaysSalesOrder, SalesOrderVariables.CREATED_AT, day_60past, TimeConstants.DATE_FORMAT_FOLDER)
      .filter(nDaysSalesOrder(SalesOrderVariables.GRAND_TOTAL).>(1000))

    val sales_45_60_df = days_45_filter.unionAll(days_60_filter)
    */

    val sales_45_60_df = last45DaySalesOrderData.unionAll(last60DaySalesOrderData)
    val sales_item_45_60_df = last45DaySalesOrderItemData.unionAll(last60DaySalesOrderItemData)

    val customerSelection = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR).getCustomerSelector(CustomerSelection.LAST_ORDER)

    val dfCustomerSelected = customerSelection.customerSelection(sales_45_60_df, sales_item_45_60_df)
    CampaignUtils.debug(dfCustomerSelected, CampaignCommon.HOTTEST_X_CAMPAIGN + "after customer selection")

    val filteredSku = Daily.skuFilter(dfCustomerSelected, yesterdayItr)

    CampaignUtils.debug(filteredSku, CampaignCommon.HOTTEST_X_CAMPAIGN + "after filteredSku ")

    CampaignUtils.campaignPostProcess(DataSets.CALENDAR_CAMPAIGNS, CampaignCommon.HOTTEST_X_CAMPAIGN, filteredSku, false,
      recommendations, incrDate)

  }

}
