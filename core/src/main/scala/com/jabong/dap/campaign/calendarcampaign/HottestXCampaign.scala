package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.Daily
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.Utils
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CustomerSelection }
import com.jabong.dap.common.constants.variables.SalesOrderVariables
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by samathashetty on 9/11/15.
 */
class HottestXCampaign {

  def runCampaign(nDaysSalesOrder: DataFrame, nDaysSalesOrderItem_60: DataFrame, yesterdayItr: DataFrame, recommendations: DataFrame, incrDate: String) {

    val day_45past = TimeUtils.getDateAfterNDays(-45, TimeConstants.DATE_FORMAT_FOLDER)

    val days_45_filter = Utils.getOneDayData(nDaysSalesOrder, SalesOrderVariables.CREATED_AT, day_45past, TimeConstants.DATE_FORMAT_FOLDER)
      .filter(nDaysSalesOrder(SalesOrderVariables.GRAND_TOTAL).<=(1000))

    val day_60past = TimeUtils.getDateAfterNDays(-60, TimeConstants.DATE_FORMAT_FOLDER)

    val days_60_filter = Utils.getOneDayData(nDaysSalesOrder, SalesOrderVariables.CREATED_AT, day_60past, TimeConstants.DATE_FORMAT_FOLDER)
      .filter(nDaysSalesOrder(SalesOrderVariables.GRAND_TOTAL).>(1000))

    val sales_45_60_df = days_45_filter.unionAll(days_60_filter)

    val customerSelection = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR).getCustomerSelector(CustomerSelection.LAST_ORDER)

    val dfCustomerSelected = customerSelection.customerSelection(sales_45_60_df, nDaysSalesOrderItem_60)
    CampaignUtils.debug(dfCustomerSelected, CampaignCommon.HOTTEST_X_CAMPAIGN + "after customer selection")

    val filteredSku = Daily.skuFilter(dfCustomerSelected, yesterdayItr)

    CampaignUtils.debug(filteredSku, CampaignCommon.HOTTEST_X_CAMPAIGN + "after filteredSku ")

    CampaignUtils.campaignPostProcess(DataSets.CALENDAR_CAMPAIGNS, CampaignCommon.HOTTEST_X_CAMPAIGN, filteredSku, false,
      recommendations)

  }

}
