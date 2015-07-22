package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ SkuSelection, CustomerSelection, CampaignCommon }
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul for com.jabong.dap.campaign.campaignlist on 18/7/15.
 */
class InvalidLowStockCampaign {
  /**
   *
   * @param customerOrderData
   * @param orderItemData
   * @param itrData
   */
  def runCampaign(customerOrderData: DataFrame, orderItemData: DataFrame, itrData: DataFrame): Unit = {

    val invalidCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.INVALID)

    //FIXME:Filter the order items data for last 30 days
    val selectedCustomers = invalidCustomerSelector.customerSelection(customerOrderData, orderItemData)

    //sku selection
    val followUp = CampaignProducer.getFactory(CampaignCommon.SKU_SELECTOR).getSkuSelector(SkuSelection.LOW_STOCK)
    val refSkus = followUp.skuFilter(selectedCustomers, itrData)

    //save campaign Output
    CampaignOutput.saveCampaignData(refSkus, CampaignCommon.BASE_PATH + "/"
      + CampaignCommon.INVALID_LOWSTOCK_CAMPAIGN + "/" + CampaignUtils.now(CampaignCommon.DATE_FORMAT))

  }
}
