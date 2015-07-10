package com.jabong.dap.campaign.campaignlist

import org.apache.spark.sql.DataFrame

class WishlistLowStockCampaign {
  def runCampaign(inData: DataFrame): Unit = {
    // select customers who have added one or more items to wishlist during 30 days

    // sku filter
    // 1. order should not have been placed for the ref sku yet
    // 2. Quantity of sku (SIMPLE- include size) falls is less than/equal to 10
    // 3. pick n ref based on special price (descending)
    // 4. This campaign should not have gone to the customer in the past 30 days for the same Ref SKU

    // null recommendation

  }
}
