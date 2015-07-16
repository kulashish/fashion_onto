package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.common.constants.campaign.{CustomerSelection, CampaignCommon}

/**
 * Created by rahul for invalid follow up campaign campaignlist on 16/7/15.
 */
class InvalidFollowUpCampaign {

  val returnCancelCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
    .getCustomerSelector(CustomerSelection.RETURN_CANCEL)

}
