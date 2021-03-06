package com.jabong.dap.campaign.customerselection

import com.jabong.dap.campaign.manager.CampaignFactory
import com.jabong.dap.campaign.recommendation.Recommender
import com.jabong.dap.common.constants.campaign.CustomerSelection

/**
 * Created by rahul for customer selection factory on 6/7/15.
 */
class CustomerSelectorFactory extends CampaignFactory {

  override def getCustomerSelector(customerSelectionType: String): CustomerSelector = {
    if (customerSelectionType == null) {
      return null
    }

    if (customerSelectionType.equalsIgnoreCase(CustomerSelection.RETURN_CANCEL)) {
      return new ReturnCancel()
    }

    if (customerSelectionType.equalsIgnoreCase(CustomerSelection.INVALID)) {
      return new Invalid()
    }

    if (customerSelectionType.equalsIgnoreCase(CustomerSelection.ACART)) {
      return new ACart()
    }

    if (customerSelectionType.equalsIgnoreCase(CustomerSelection.WISH_LIST)) {
      return new WishList()
    }

    if (customerSelectionType.equalsIgnoreCase(CustomerSelection.YESTERDAY_SESSION)) {
      return new YesterdaySession()
    }

    if (customerSelectionType.equalsIgnoreCase(CustomerSelection.YESTERDAY_SESSION_DISTINCT)) {
      return new YesterdaySessionDistinct()
    }

    if (customerSelectionType.equalsIgnoreCase(CustomerSelection.LAST_THIRTY_DAY_SESSION)) {
      return new LastThirtyDaySession()
    }
    if (customerSelectionType.equalsIgnoreCase(CustomerSelection.SALES_CART)) {
      return new SalesCart()
    }
    if (customerSelectionType.equalsIgnoreCase(CustomerSelection.CLOSED_ORDER)) {
      return new ClosedOrder()
    }
    if (customerSelectionType.equalsIgnoreCase(CustomerSelection.CUSTOMER_PREFERRED_DATA)) {
      return new CustomerPreferredData()
    }
    if (customerSelectionType.equalsIgnoreCase(CustomerSelection.LAST_ORDER)) {
      return new LastOrder()
    }
    if (customerSelectionType.equalsIgnoreCase(CustomerSelection.LAST5_SUCCESSFUL_ORDER)) {
      return new Last5SuccessfulOrder()
    }

    return null
  }

  override def getRecommender(recType: String): Recommender = ???

}
