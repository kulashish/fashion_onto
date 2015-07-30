package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ SkuSelection, CustomerSelection, CampaignCommon }
import com.jabong.dap.common.constants.variables.{ ProductVariables, ItrVariables, CustomerProductShortlistVariables }
import com.jabong.dap.common.udf.Udf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class WishlistFollowupCampaign {
  def runCampaign(customerSelected: DataFrame, itrSkuYesterdayData: DataFrame, itrSkuSimpleYesterdayData: DataFrame, orderData: DataFrame, orderItemData: DataFrame): Unit = {

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
    val dfUnion = skuOnlyRecords.unionAll(skuSimpleOnlyRecords).select(
      col(CustomerProductShortlistVariables.FK_CUSTOMER),
      col(CustomerProductShortlistVariables.SKU) as CustomerProductShortlistVariables.SKU_SIMPLE,
      col(CustomerProductShortlistVariables.SPECIAL_PRICE)
    )

    val refSkus = CampaignUtils.generateReferenceSku(dfUnion, CampaignCommon.NUMBER_REF_SKUS)

    val campaignOutput = CampaignUtils.addCampaignMailType(refSkus, CampaignCommon.WISHLIST_FOLLOWUP_CAMPAIGN)

    //save campaign Output
    CampaignOutput.saveCampaignDataForYesterday(campaignOutput, CampaignCommon.WISHLIST_FOLLOWUP_CAMPAIGN)

  }

}