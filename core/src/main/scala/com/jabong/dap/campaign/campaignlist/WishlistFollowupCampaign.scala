package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.skuselection.Wishlist
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, SkuSelection }
import com.jabong.dap.common.constants.variables.{ ProductVariables, CustomerProductShortlistVariables }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class WishlistFollowupCampaign {
  def runCampaign(customerSelected: DataFrame, itrSkuYesterdayData: DataFrame, itrSkuSimpleYesterdayData: DataFrame, orderData: DataFrame, orderItemData: DataFrame, brickMvpRecommendations: DataFrame): Unit = {

    // select customers who have added one or more items to wishlist during last day

    // sku filter 
    // 1. order should not have been placed for the ref sku yet
    // 2. pick based on special price (descending)

    // null recommendation

    // data will contain both sku and sku simple records

    // list1 filter only sku and join it with last day itr ---> output fk_customer, sku, price
    val skuOnlyRecords = Wishlist.skuSelector(customerSelected, itrSkuYesterdayData, null, orderData, orderItemData, SkuSelection.FOLLOW_UP)

    // list2 filter only sku-simple and join it with last day itr ---> output fk_customer, sku, price
    val skuSimpleOnlyRecords = Wishlist.skuSimpleSelector(customerSelected, itrSkuSimpleYesterdayData, orderData, orderItemData, SkuSelection.FOLLOW_UP)

    // union list1 and list2, group by customer, order by price, first/last
    //=======union both sku and sku simple==============================================================================
    val dfUnion = skuOnlyRecords.unionAll(skuSimpleOnlyRecords).select(
      col(CustomerProductShortlistVariables.FK_CUSTOMER),
      col(CustomerProductShortlistVariables.SKU) as CustomerProductShortlistVariables.SKU_SIMPLE,
      col(CustomerProductShortlistVariables.SPECIAL_PRICE),
      col(ProductVariables.BRAND),
      col(ProductVariables.BRICK),
      col(ProductVariables.MVP),
      col(ProductVariables.GENDER)
    )

    // ***** mobile push use case
    CampaignUtils.campaignPostProcess(DataSets.PUSH_CAMPAIGNS, CampaignCommon.WISHLIST_FOLLOWUP_CAMPAIGN, dfUnion, false)

    // ***** email use case
    CampaignUtils.campaignPostProcess(DataSets.EMAIL_CAMPAIGNS, CampaignCommon.WISHLIST_FOLLOWUP_CAMPAIGN, dfUnion, false, brickMvpRecommendations)
  }

}