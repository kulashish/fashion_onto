package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.HottestXFilter
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{CustomerSelection, CampaignCommon}
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by samathashetty on 9/11/15.
 */
class HottestXCampaign {

  def runHottestXCampaign(nthDay_45Df : DataFrame, nthDay_60Df : DataFrame, recommendations: DataFrame) {

    val customerSelection =  CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.HOTTEST_X)

    val hottestX = customerSelection.customerSelection(nthDay_45Df, nthDay_60Df)

    val filteredSku  = hottestX//HottestXFilter.skuFilter(hottestX, yesterdayItr)

    CampaignUtils.campaignPostProcess(DataSets.CALENDAR_CAMPAIGNS, CampaignCommon.HOTTEST_X, filteredSku, false,
      recommendations)
  }

}
