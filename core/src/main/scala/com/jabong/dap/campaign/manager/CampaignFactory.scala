package com.jabong.dap.campaign.manager

import com.jabong.dap.campaign.actions.Action
import com.jabong.dap.campaign.recommendation.Recommender
import com.jabong.dap.campaign.customerselect.CustomerSelection

/**
 * Created by jabong1145 on 7/7/15.
 */

 abstract class CampaignFactory {

  def getCustomer(customerType:String): CustomerSelection

  def getAction(action:String):Action

  def getRecommendation(recType:String): Recommender

}
