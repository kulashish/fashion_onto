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

    //filter sku based on iod filter
    val filteredSku = iod.skuFilter(custFiltered, last30daysItrData)

    //generate reference sku for acart with acart url
    val refSkus = CampaignUtils.generateReferenceSkusForAcart(filteredSku, CampaignCommon.NUMBER_REF_SKUS)

    val campaignOutput = CampaignUtils.addCampaignMailType(refSkus, CampaignCommon.ACART_IOD_CAMPAIGN)

    //save campaign Output
    CampaignOutput.saveCampaignDataForYesterday(campaignOutput, CampaignCommon.ACART_IOD_CAMPAIGN)

  }

}
