package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ SkuSelection, CustomerSelection, CampaignCommon }
import com.jabong.dap.common.constants.variables.{ProductVariables, ItrVariables, CustomerProductShortlistVariables}
import com.jabong.dap.common.udf.Udf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class WishlistFollowupCampaign {
  def runCampaign(customerSelected: DataFrame, itrSkuYesterdayData: DataFrame, itrSkuSimpleYesterdayData: DataFrame, orderData:DataFrame, orderItemData:DataFrame): Unit = {

    // select customers who have added one or more items to wishlist during last day

    // sku filter 
    // 1. order should not have been placed for the ref sku yet
    // 2. pick based on special price (descending)

    // null recommendation

    // data will contain both sku and sku simple records
    
    // list1 filter only sku and join it with last day itr ---> output fk_customer, sku, price
    val skuOnlyRecords = WishListCampaign.skuSelector(customerSelected, itrSkuYesterdayData, null, orderData, orderItemData, WishListCampaign.FOLLOW_UP)
    
    // list2 filter only sku-simple and join it with last day itr ---> output fk_customer, sku, price
    val skuSimpleOnlyRecords = WishListCampaign.skuSimpleSelector(customerSelected, itrSkuSimpleYesterdayData, orderData, orderItemData, WishListCampaign.FOLLOW_UP)
    
    // union list1 and list2, group by customer, order by price, first/last
    //=======union both sku and sku simple==============================================================================
    val dfUnion = skuOnlyRecords.unionAll(skuSimpleOnlyRecords)

    val refSkus = CampaignUtils.generateReferenceSku(dfUnion, CampaignCommon.NUMBER_REF_SKUS)

    val campaignOutput = CampaignUtils.addCampaignMailType(refSkus, CampaignCommon.WISHLIST_FOLLOWUP_CAMPAIGN)
    
    //save campaign Output
    CampaignOutput.saveCampaignData(campaignOutput, CampaignCommon.BASE_PATH + "/"
      + CampaignCommon.WISHLIST_FOLLOWUP_CAMPAIGN + "/" + CampaignUtils.now(CampaignCommon.DATE_FORMAT))

  }


  // wishlist low stock - 30 days wishlist data, last 30 days order item, 30 days order, last day itr
  def runCampaignLowStock(shortListFullData: DataFrame,itrSku30DaysData:DataFrame,itrSkuSimpleYesterdayData:DataFrame): Unit = {
    // select customers who have added one or more items to wishlist during 30 days

    // sku filter
    // 1. order should not have been placed for the ref sku yet
    // 2. Quantity of sku (SIMPLE- include size) falls is less than/equal to 10
    // 3. pick n ref based on special price (descending)
    // 4. This campaign should not have gone to the customer in the past 30 days for the same Ref SKU

    // null recommendation

    val wishListCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.WISH_LIST)
    val customerSelected = wishListCustomerSelector.customerSelection(shortListFullData, 30)


    // data will contain both sku and sku simple records

    // list1 filter only sku and join it with last day itr ---> output fk_customer, sku, price
    val skuOnlyRecords = skuSelector(customerSelected, itrSkuSimpleYesterdayData, true)

    // list2 filter only sku-simple and join it with last day itr ---> output fk_customer, sku, price
    val skuSimpleOnlyRecords = skuSimpleSelector(customerSelected, true)

    // union list1 and list2, group by customer, order by price, first/last
    //=======union both sku and sku simple==============================================================================
    val dfUnion = skuOnlyRecords.unionAll(skuSimpleOnlyRecords)

    val refSkus = CampaignUtils.generateReferenceSku(dfUnion, CampaignCommon.NUMBER_REF_SKUS)

    val campaignOutput = CampaignUtils.addCampaignMailType(refSkus, CampaignCommon.WISHLIST_LOWSTOCK_CAMPAIGN)
    //save campaign Output
    CampaignOutput.saveCampaignData(campaignOutput, CampaignCommon.BASE_PATH + "/"
      + CampaignCommon.WISHLIST_LOWSTOCK_CAMPAIGN + "/" + CampaignUtils.now(CampaignCommon.DATE_FORMAT))
  }

}