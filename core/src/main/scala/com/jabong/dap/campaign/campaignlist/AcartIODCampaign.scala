package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.ItemOnDiscount
import com.jabong.dap.campaign.traceability.PastCampaignCheck
import com.jabong.dap.campaign.utils
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CustomerSelection, SkuSelection }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame
import scala.annotation._, elidable._
/**
 * Created by rahul for com.jabong.dap.campaign.campaignlist on 20/7/15.
 */
class AcartIODCampaign {

  def runCampaign(last30DayAcartData: DataFrame, last30daySalesOrderData: DataFrame, last30DaySalesOrderItemData: DataFrame, last30daysItrData: DataFrame, brickMvpRecommendations: DataFrame): Unit = {

    val acartCustomerSelection = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.ACART)
    //FIXME:Filter the order items data for 30 days
    val selectedCustomers = acartCustomerSelection.customerSelection(last30DayAcartData, last30daySalesOrderData, last30DaySalesOrderItemData)

    CampaignUtils.debug(selectedCustomers, "AcartIODCampaigns selected Customer ")
    //sku selection
    //filter sku based on iod filter
    val filteredSku = ItemOnDiscount.skuFilter(selectedCustomers, last30daysItrData).cache()

    CampaignUtils.debug(filteredSku, "AcartIODCampaigns filteredSku ")

    // ***** mobile push use case
    CampaignUtils.campaignPostProcess(DataSets.PUSH_CAMPAIGNS, CampaignCommon.ACART_IOD_CAMPAIGN, filteredSku)

    // ***** email use case
    CampaignUtils.campaignPostProcess(DataSets.EMAIL_CAMPAIGNS, CampaignCommon.ACART_IOD_CAMPAIGN, filteredSku, true, brickMvpRecommendations)

  }

}
