package com.jabong.dap.campaign.skuselection

import com.jabong.dap.campaign.customerselection.CustomerSelector
import com.jabong.dap.campaign.manager.CampaignFactory
import com.jabong.dap.campaign.recommendation.Recommender
import com.jabong.dap.common.constants.campaign.SkuSelection

class SkuSelectorFactory extends CampaignFactory {
  override def getCustomerSelector(customerType: String): CustomerSelector = ???

  override def getSkuSelector(actionType: String): SkuSelector = {

    if (actionType == null) {
      return null
    }

    if (actionType.equalsIgnoreCase(SkuSelection.CANCEL_RETARGET)) {
      return new CancelReTarget()
    }

    if (actionType.equalsIgnoreCase(SkuSelection.RETURN_RETARGET)) {
      return new ReturnReTarget()
    }

    if (actionType.equalsIgnoreCase(SkuSelection.ITEM_ON_DISCOUNT)) {
      return new ItemOnDiscount()
    }

    if (actionType.equalsIgnoreCase(SkuSelection.LOW_STOCK)) {
      return new LowStock()
    }

    return null
  }

  override def getRecommender(recType: String): Recommender = ???
}
