package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CustomerSelection }
import org.apache.spark.sql.DataFrame

/**
 * Created by jabong1142 on 8/7/15.
 */
class LiveRetargetCampaign {

  def runCampaign(inData: DataFrame): Unit = {

    // x = run retargeting campaign common customer selection
    val returnCancelCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR).getCustomerSelector(CustomerSelection.RETURN_CANCEL)

    // find customers with required order-item status during last day
    val targetCustomersWithOrderItems = returnCancelCustomerSelector.customerSelection(null, null)

    // run cancel retargeting
    val cancelCampaign = new LiveCancelReTargetCampaign()
    cancelCampaign.runCampaign(targetCustomersWithOrderItems)

    // run return retargeting

    // (x)

  }
}
