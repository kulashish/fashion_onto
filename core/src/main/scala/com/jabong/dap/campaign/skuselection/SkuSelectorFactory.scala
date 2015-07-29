package com.jabong.dap.campaign.skuselection

import com.jabong.dap.campaign.customerselection.CustomerSelector
import com.jabong.dap.campaign.manager.CampaignFactory
import com.jabong.dap.campaign.recommendation.Recommender
import com.jabong.dap.common.constants.campaign.SkuSelection

class SkuSelectorFactory extends CampaignFactory {

  override def getCustomerSelector(customerType: String): CustomerSelector = ???

  override def getSkuSelector(skuSelectorType: String): SkuSelector = {
    if (skuSelectorType == null) {
      return null
    }
    if (skuSelectorType.equalsIgnoreCase(SkuSelection.CANCEL_RETARGET)) {
      return new CancelReTarget()
    }

    if (skuSelectorType.equalsIgnoreCase(SkuSelection.RETURN_RETARGET)) {
      return new ReturnReTarget()
    }

    if (skuSelectorType.equalsIgnoreCase(SkuSelection.FOLLOW_UP)) {
      return new FollowUp()
    }

    if (skuSelectorType.equalsIgnoreCase(SkuSelection.LOW_STOCK)) {
      return new LowStock()
    }

    if (skuSelectorType.equalsIgnoreCase(SkuSelection.SKU_LOW_STOCK)) {
      return new SkuLowStock()
    }

    if (skuSelectorType.equalsIgnoreCase(SkuSelection.ITEM_ON_DISCOUNT)) {
      return new ItemOnDiscount()
    }

    if (skuSelectorType.equalsIgnoreCase(SkuSelection.SKU_ITEM_ON_DISCOUNT)) {
      return new SkuItemOnDiscount()
    }

    if (skuSelectorType.equalsIgnoreCase(SkuSelection.DAILY)) {
      return new Daily()
    }

    if (skuSelectorType.equalsIgnoreCase(SkuSelection.SURF)) {
      return new Surf()
    }

    return null
  }

  override def getRecommender(recType: String): Recommender = ???

}
