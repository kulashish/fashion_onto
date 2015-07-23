package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ SkuSelection, CustomerSelection, CampaignCommon }
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul for com.jabong.dap.campaign.campaignlist on 20/7/15.
 */
class AcartLowStockCampaign {

  def runCampaign(last30DayAcartData: DataFrame, last30DaySalesOrderData: DataFrame, last30DaySalesOrderItemData: DataFrame, yesterdayItrData: DataFrame): Unit = {

    val acartCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.ACART)
    //FIXME:Filter the order items data for last day
    val selectedCustomers = acartCustomerSelector.customerSelection(last30DayAcartData, last30DaySalesOrderData, last30DaySalesOrderItemData)

    //sku selection
    val lowStock = CampaignProducer.getFactory(CampaignCommon.SKU_SELECTOR).getSkuSelector(SkuSelection.LOW_STOCK)
    val refSkus = lowStock.skuFilter(selectedCustomers, yesterdayItrData)

    //save campaign Output
    CampaignOutput.saveCampaignData(refSkus, CampaignCommon.BASE_PATH + "/"
      + CampaignCommon.ACART_LOWSTOCK_CAMPAIGN + "/" + CampaignUtils.now(CampaignCommon.DATE_FORMAT))

  }

}