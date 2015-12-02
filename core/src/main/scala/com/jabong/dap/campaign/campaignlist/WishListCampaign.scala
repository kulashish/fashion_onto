package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.common.constants.campaign.{CampaignCommon, CustomerSelection}
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul for com.jabong.dap.campaign.campaignlist on 27/7/15.
 */
class WishListCampaign {

  def runCampaign(shortlistYesterdayData: DataFrame,
                  shortlistLast30DayData: DataFrame,
                  itrSkuYesterdayData: DataFrame,
                  itrSkuSimpleYesterdayData: DataFrame,
                  yesterdaySalesOrderData: DataFrame,
                  yesterdaySalesOrderItemData: DataFrame,
                  last30DaySalesOrderData: DataFrame,
                  last30DaySalesOrderItemData: DataFrame,
                  itrSku30DayData: DataFrame,
                  brickMvpRecommendations: DataFrame,
                  incrDate: String): Unit = {

    val wishListCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.WISH_LIST)
    val lastDayCustomerSelected = wishListCustomerSelector.customerSelection(shortlistYesterdayData)
    val last30DaysCustomerSelected = wishListCustomerSelector.customerSelection(shortlistLast30DayData)

    val wishlistFollowupCampaign = new WishlistFollowupCampaign()
    wishlistFollowupCampaign.runCampaign(lastDayCustomerSelected, itrSkuYesterdayData, itrSkuSimpleYesterdayData, yesterdaySalesOrderData, yesterdaySalesOrderItemData, brickMvpRecommendations, incrDate)

    val wishListLowStockCampaign = new WishlistLowStockCampaign()
    wishListLowStockCampaign.runCampaign(last30DaysCustomerSelected, itrSkuYesterdayData, itrSkuSimpleYesterdayData, last30DaySalesOrderData, last30DaySalesOrderItemData, brickMvpRecommendations, incrDate)

    val wishListIODCampaign = new WishlistIODCampaign()
    wishListIODCampaign.runCampaign(last30DaysCustomerSelected, itrSkuYesterdayData, itrSku30DayData, itrSkuSimpleYesterdayData, last30DaySalesOrderData, last30DaySalesOrderItemData, brickMvpRecommendations, incrDate)

  }

}
