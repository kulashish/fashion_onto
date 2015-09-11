package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CustomerSelection }
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 11/9/15.
 */
object MIPRCampaign {

  def runCampaign(salesOrder30DayData: DataFrame, salesOrderItemYesterdayData: DataFrame, recommendationsData: DataFrame, yesterdayItrData: DataFrame) = {

    val salesOrderCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.SALES_ORDER)

    val dfCustomerSelection = salesOrderCustomerSelector.customerSelection(salesOrder30DayData, salesOrderItemYesterdayData)

    //TODO: Fix recommendation Data

    //TODO: generate reference skus
    //    val refSkus = CampaignUtils.generateReferenceSkuForSurf(skus, 1)

  }

}