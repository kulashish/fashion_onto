package com.jabong.dap.campaign.campaignlist

import org.apache.spark.sql.DataFrame

class WishlistFollowupCampaign {
  def runCampaign(inData: DataFrame): Unit = {
    
    // select customers who have added one or more items to wishlist during last day
    
    // sku filter 
    // 1. order should not have been placed for the ref sku yet
    // 2. pick based on special price (descending)

    
    // null recommendation
    
  }
}