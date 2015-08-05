package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.traceability.PastCampaignCheck
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ SkuSelection, CustomerSelection, CampaignCommon }
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul for com.jabong.dap.campaign.campaignlist on 20/7/15.
 */
class AcartFollowUpCampaign {

  def runCampaign(past30DayCampaignMergedData: DataFrame, prev3rdDayAcartData: DataFrame, last3DaySalesOrderData: DataFrame, last3DaySalesOrderItemData: DataFrame, itrData: DataFrame): Unit = {

    val acartCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.ACART)
    //FIXME:Filter the order items data for last 3 days
    val selectedCustomers = acartCustomerSelector.customerSelection(prev3rdDayAcartData, last3DaySalesOrderData, last3DaySalesOrderItemData)

    //past campaign check whether the campaign has been sent to customer in last 30 days
    val pastCampaignCheck = new PastCampaignCheck()
    val custFiltered = pastCampaignCheck.campaignRefSkuCheck(past30DayCampaignMergedData, selectedCustomers,
      CampaignCommon.campaignMailTypeMap.getOrElse(CampaignCommon.ACART_FOLLOWUP_CAMPAIGN, 1000), 30)

    //sku selection
    val followUp = CampaignProducer.getFactory(CampaignCommon.SKU_SELECTOR).getSkuSelector(SkuSelection.FOLLOW_UP)
    val filteredSku = followUp.skuFilter(selectedCustomers, itrData)

    val refSkus = CampaignUtils.generateReferenceSkusForAcart(filteredSku, CampaignCommon.NUMBER_REF_SKUS)

    val campaignOutput = CampaignUtils.addCampaignMailType(refSkus, CampaignCommon.ACART_FOLLOWUP_CAMPAIGN)

    //save campaign Output
    CampaignOutput.saveCampaignDataForYesterday(campaignOutput, CampaignCommon.ACART_FOLLOWUP_CAMPAIGN)

  }
}
