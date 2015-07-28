package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ SkuSelection, CustomerSelection, CampaignCommon }
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul for com.jabong.dap.campaign.campaignlist on 20/7/15.
 */
class AcartIODCampaign {

  def runCampaign(last30DayAcartData: DataFrame, last30daySalesOrderData: DataFrame, last30DaySalesOrderItemData: DataFrame, last30daysItrData: DataFrame): Unit = {

    val acartCustomerSelection = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.ACART)
    //FIXME:Filter the order items data for 30 days
    val selectedCustomers = acartCustomerSelection.customerSelection(last30DayAcartData, last30daySalesOrderData, last30DaySalesOrderItemData)

    //sku selection
    val iod = CampaignProducer.getFactory(CampaignCommon.SKU_SELECTOR).getSkuSelector(SkuSelection.ITEM_ON_DISCOUNT)
    val refSkus = iod.skuFilter(selectedCustomers, last30daysItrData)

    val campaignOutput = CampaignUtils.addCampaignMailType(refSkus, CampaignCommon.ACART_IOD_CAMPAIGN)

    //save campaign Output
    CampaignOutput.saveCampaignData(campaignOutput, CampaignCommon.BASE_PATH + "/"
      + CampaignCommon.ACART_IOD_CAMPAIGN + "/" + CampaignUtils.now(CampaignCommon.DATE_FORMAT))

  }

}
