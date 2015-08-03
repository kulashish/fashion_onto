package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.campaign.traceability.PastCampaignCheck
import com.jabong.dap.common.constants.campaign.{ SkuSelection, CustomerSelection, CampaignCommon }
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul for invalid follow up campaign campaignlist on 16/7/15.
 */
class InvalidFollowUpCampaign {

  def runCampaign(past30DayCampaignMergedData: DataFrame, customerOrderData: DataFrame, orderItemData: DataFrame, itrData: DataFrame): Unit = {

    val invalidCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.INVALID)
    //FIXME:Filter the order items data for 3 days
    val selectedCustomers = invalidCustomerSelector.customerSelection(customerOrderData, orderItemData)

    var custFiltered = selectedCustomers

    if (past30DayCampaignMergedData != null) {
      //past campaign check whether the campaign has been sent to customer in last 30 days
      val pastCampaignCheck = new PastCampaignCheck()
      custFiltered = pastCampaignCheck.campaignCheck(past30DayCampaignMergedData, selectedCustomers,
        CampaignCommon.campaignMailTypeMap.getOrElse(CampaignCommon.INVALID_FOLLOWUP_CAMPAIGN, 1000), 30)

    }

    //sku selection
    val followUp = CampaignProducer.getFactory(CampaignCommon.SKU_SELECTOR).getSkuSelector(SkuSelection.FOLLOW_UP)
    val refSkus = followUp.skuFilter(custFiltered, itrData)

    val campaignOutput = CampaignUtils.addCampaignMailType(refSkus, CampaignCommon.INVALID_FOLLOWUP_CAMPAIGN)

    //save campaign Output
    CampaignOutput.saveCampaignDataForYesterday(campaignOutput, CampaignCommon.INVALID_FOLLOWUP_CAMPAIGN)

  }

}
