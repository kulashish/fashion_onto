package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.Wishlist
import com.jabong.dap.campaign.traceability.PastCampaignCheck
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, SkuSelection }
import com.jabong.dap.common.constants.variables.CustomerProductShortlistVariables
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class WishlistIODCampaign {

  // wishlist iod stock - 30 days wishlist data, last 30 days order item, 30 days order, last day itr, 30 day itr sku
  def runCampaign(past30DayCampaignMergedData: DataFrame, customerSelected: DataFrame, itrSkuYesterdayData: DataFrame, itrSku30DayData: DataFrame, itrSkuSimpleYesterdayData: DataFrame, orderData: DataFrame, orderItemData: DataFrame): Unit = {
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
      col(CustomerProductShortlistVariables.FK_CUSTOMER),
      col(CustomerProductShortlistVariables.SKU) as CustomerProductShortlistVariables.SKU_SIMPLE,
      col(CustomerProductShortlistVariables.SPECIAL_PRICE)
    )

    var skusFiltered = dfUnion

    if (past30DayCampaignMergedData != null) {
      //past campaign check whether the campaign has been sent to customer in last 30 days
      skusFiltered = PastCampaignCheck.campaignRefSkuCheck(past30DayCampaignMergedData, dfUnion,
        CampaignCommon.campaignMailTypeMap.getOrElse(CampaignCommon.WISHLIST_IOD_CAMPAIGN, 1000), 30)
    }
    val refSkus = CampaignUtils.generateReferenceSku(skusFiltered, CampaignCommon.NUMBER_REF_SKUS)

    val campaignOutput = CampaignUtils.addCampaignMailType(refSkus, CampaignCommon.WISHLIST_IOD_CAMPAIGN)
    //save campaign Output
    CampaignOutput.saveCampaignDataForYesterday(campaignOutput, CampaignCommon.WISHLIST_IOD_CAMPAIGN)

  }

}
