package com.jabong.dap.campaign.manager

import com.jabong.dap.campaign.recommendation.Recommender
import com.jabong.dap.campaign.customerselection.CustomerSelector

/**
 *  A campaign consists of three main stages:
 *  1. Customer selection --- e.g., all users who have abandoned cart during last 30 days
 *  2. Reference SKU selection -- e.g., out of abandoned skus, find skus having stock of less than 10
 *  3. Recommended SKU selection -- i.e., given the customer and reference skus, find the list of recommended skus
 *
 *  This abstract factory interface defines methods for all these steps, which will be
 *  be implemented by Factory instances of each stage separately.
 */

abstract class CampaignFactory {

  def getCustomerSelector(customerType: String): CustomerSelector

  def getRecommender(recType: String): Recommender

}
