package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.{CampaignInput, CampaignOutput}
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.traceability.PastCampaignCheck
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{Recommendation, SkuSelection, CustomerSelection, CampaignCommon}
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul for com.jabong.dap.campaign.campaignlist on 20/7/15.
 */
class AcartIODCampaign {

  def runCampaign(past30DayCampaignMergedData: DataFrame, last30DayAcartData: DataFrame, last30daySalesOrderData: DataFrame, last30DaySalesOrderItemData: DataFrame, last30daysItrData: DataFrame): Unit = {

    val acartCustomerSelection = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.ACART)
    //FIXME:Filter the order items data for 30 days
    val selectedCustomers = acartCustomerSelection.customerSelection(last30DayAcartData, last30daySalesOrderData, last30DaySalesOrderItemData)

    var custFiltered = selectedCustomers

    if (past30DayCampaignMergedData != null) {
      //past campaign check whether the campaign has been sent to customer in last 30 days
      val pastCampaignCheck = new PastCampaignCheck()
      custFiltered = pastCampaignCheck.campaignRefSkuCheck(past30DayCampaignMergedData, selectedCustomers,
        CampaignCommon.campaignMailTypeMap.getOrElse(CampaignCommon.ACART_IOD_CAMPAIGN, 1000), 30)

    }
    //sku selection
    val iod = CampaignProducer.getFactory(CampaignCommon.SKU_SELECTOR).getSkuSelector(SkuSelection.ITEM_ON_DISCOUNT)
    val refSkus = iod.skuFilter(custFiltered, last30daysItrData)

    val campaignOutput = CampaignUtils.addCampaignMailType(refSkus, CampaignCommon.ACART_IOD_CAMPAIGN)
    val commonRecommender = CampaignProducer.getFactory(CampaignCommon.RECOMMENDER).getRecommender(Recommendation.LIVE_COMMON_RECOMMENDER)
    val yesterdaySkuData = CampaignInput.loadYesterdayItrSkuData()
    commonRecommender.generateRecommendation(last30DaySalesOrderItemData,yesterdaySkuData)
    //save campaign Output
    CampaignOutput.saveCampaignDataForYesterday(campaignOutput, CampaignCommon.ACART_IOD_CAMPAIGN)

  }

}
