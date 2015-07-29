package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ SkuSelection, CustomerSelection, CampaignCommon }
import com.jabong.dap.common.time.TimeConstants
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul for invalid follow up campaign campaignlist on 16/7/15.
 */
class InvalidFollowUpCampaign {

  def runCampaign(customerOrderData: DataFrame, orderItemData: DataFrame, itrData: DataFrame): Unit = {

    val invalidCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.INVALID)
    //FIXME:Filter the order items data for 3 days
    val selectedCustomers = invalidCustomerSelector.customerSelection(customerOrderData, orderItemData)

    //sku selection
    val followUp = CampaignProducer.getFactory(CampaignCommon.SKU_SELECTOR).getSkuSelector(SkuSelection.FOLLOW_UP)
    val refSkus = followUp.skuFilter(selectedCustomers, itrData)

    //save campaign Output
    CampaignOutput.saveCampaignData(refSkus, CampaignCommon.BASE_PATH + "/"
      + CampaignCommon.INVALID_FOLLOWUP_CAMPAIGN + "/" + CampaignUtils.now(TimeConstants.DATE_FORMAT_FOLDER))

  }

}
