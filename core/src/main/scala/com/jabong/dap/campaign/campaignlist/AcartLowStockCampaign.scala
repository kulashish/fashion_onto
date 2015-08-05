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
class AcartLowStockCampaign {

  def runCampaign(past30DayCampaignMergedData: DataFrame, last30DayAcartData: DataFrame, last30DaySalesOrderData: DataFrame, last30DaySalesOrderItemData: DataFrame, yesterdayItrData: DataFrame): Unit = {

    val acartCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.ACART)
    //FIXME:Filter the order items data for last day
    val selectedCustomers = acartCustomerSelector.customerSelection(last30DayAcartData, last30DaySalesOrderData, last30DaySalesOrderItemData)

    var custFiltered = selectedCustomers

    if (past30DayCampaignMergedData != null) {
      //past campaign check whether the campaign has been sent to customer in last 30 days
      val pastCampaignCheck = new PastCampaignCheck()
      custFiltered = pastCampaignCheck.campaignCheck(past30DayCampaignMergedData, selectedCustomers,
        CampaignCommon.campaignMailTypeMap.getOrElse(CampaignCommon.ACART_LOWSTOCK_CAMPAIGN, 1000), 30)
    }

    //sku selection
    val lowStock = CampaignProducer.getFactory(CampaignCommon.SKU_SELECTOR).getSkuSelector(SkuSelection.LOW_STOCK)
    val filteredSku = lowStock.skuFilter(selectedCustomers, yesterdayItrData)

    val refSkus = CampaignUtils.generateReferenceSkusForAcart(filteredSku, CampaignCommon.NUMBER_REF_SKUS)

    val campaignOutput = CampaignUtils.addCampaignMailType(refSkus, CampaignCommon.ACART_LOWSTOCK_CAMPAIGN)

    //save campaign Output
    CampaignOutput.saveCampaignDataForYesterday(campaignOutput, CampaignCommon.ACART_LOWSTOCK_CAMPAIGN)

  }

}
