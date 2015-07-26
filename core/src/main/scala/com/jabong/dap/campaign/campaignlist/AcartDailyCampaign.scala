package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ SkuSelection, CustomerSelection, CampaignCommon }
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul for com.jabong.dap.campaign.campaignlist on 23/7/15.
 */
class AcartDailyCampaign {

  def runCampaign(yesterdayAcartData: DataFrame, yesterdaySalesOrderData: DataFrame, yesterdaySalesOrderItemData: DataFrame, yesterdayItrData: DataFrame): Unit = {

    val acartCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.ACART)
    //FIXME:Filter the order items data for last 1 day
    val selectedCustomers = acartCustomerSelector.customerSelection(yesterdayAcartData, yesterdaySalesOrderData, yesterdaySalesOrderItemData)

    //sku selection
    val daily = CampaignProducer.getFactory(CampaignCommon.SKU_SELECTOR).getSkuSelector(SkuSelection.DAILY)
    val refSkus = daily.skuFilter(selectedCustomers, yesterdayItrData)

    val campaignOutput = CampaignUtils.addCampaignMailType(refSkus,CampaignCommon.ACART_DAILY_CAMPAIGN)
     //save campaign Output
    CampaignOutput.saveCampaignData(campaignOutput, CampaignCommon.BASE_PATH + "/"
      + CampaignCommon.ACART_DAILY_CAMPAIGN + "/" + CampaignUtils.now(CampaignCommon.DATE_FORMAT))

  }
}

