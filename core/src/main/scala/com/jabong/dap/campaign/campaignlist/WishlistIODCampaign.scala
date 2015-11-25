package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.skuselection.Wishlist
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, SkuSelection }
import com.jabong.dap.common.constants.variables.{ ProductVariables, CustomerVariables }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class WishlistIODCampaign {

  // wishlist iod stock - 30 days wishlist data, last 30 days order item, 30 days order, last day itr, 30 day itr sku
  def runCampaign(customerSelected: DataFrame, itrSkuYesterdayData: DataFrame, itrSku30DayData: DataFrame, itrSkuSimpleYesterdayData: DataFrame, orderData: DataFrame, orderItemData: DataFrame, brickMvpRecommendations: DataFrame): Unit = {
    // select customers who have added one or more items to wishlist during 30 days

    // sku filter
    // 1. order should not have been placed for the ref sku yet
    // 2. Quantity of sku (SIMPLE- include size) falls is less than/equal to 10
    // 3. pick n ref based on special price (descending)
    // 4. This campaign should not have gone to the customer in the past 30 days for the same Ref SKU

    // null recommendation

    //    val wishListCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
    //      .getCustomerSelector(CustomerSelection.WISH_LIST)
    //    val customerSelected = wishListCustomerSelector.customerSelection(shortListFullData, 30)

    // data will contain both sku and sku simple records

    // list1 filter only sku and join it with last day itr ---> output fk_customer, sku, price
    val skuOnlyRecords = Wishlist.skuSelector(customerSelected, itrSkuYesterdayData, itrSku30DayData, orderData, orderItemData, SkuSelection.ITEM_ON_DISCOUNT)

    // list2 filter only sku-simple and join it with last day itr ---> output fk_customer, sku, price
    val skuSimpleOnlyRecords = Wishlist.skuSimpleSelector(customerSelected, itrSkuSimpleYesterdayData, orderData, orderItemData, SkuSelection.ITEM_ON_DISCOUNT)

    // union list1 and list2, group by customer, order by price, first/last
    //=======union both sku and sku simple==============================================================================
    val dfUnion = skuOnlyRecords.unionAll(skuSimpleOnlyRecords).select(
      col(CustomerVariables.FK_CUSTOMER),
      col(CustomerVariables.EMAIL),
      col(CustomerVariables.SKU) as CustomerVariables.SKU_SIMPLE,
      col(CustomerVariables.SPECIAL_PRICE),
      col(ProductVariables.BRAND),
      col(ProductVariables.BRICK),
      col(ProductVariables.MVP),
      col(ProductVariables.GENDER),
      col(ProductVariables.PRODUCT_NAME)

    ).cache()

    // ***** mobile push use case
    CampaignUtils.campaignPostProcess(DataSets.PUSH_CAMPAIGNS, CampaignCommon.WISHLIST_IOD_CAMPAIGN, dfUnion)

    // ***** email use case
    CampaignUtils.campaignPostProcess(DataSets.EMAIL_CAMPAIGNS, CampaignCommon.WISHLIST_IOD_CAMPAIGN, dfUnion, true, brickMvpRecommendations)

  }

}
