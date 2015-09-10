package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CustomerSelection }

/**
 * Created by rahul for com.jabong.dap.campaign.campaignlist on 27/7/15.
 */
object WishListCampaign {

  def runCampaign(): Unit = {

    // wishlist followup - 1 day wishlist data, last day order item, last day order, 

    val fullOrderData = CampaignInput.loadFullOrderData()
    val fullOrderItemData = CampaignInput.loadFullOrderItemData()

    val fullShortlistData = CampaignInput.loadFullShortlistData()

    val last30DaySalesOrderItemData = CampaignInput.loadLastNdaysOrderItemData(30, fullOrderItemData) // created_at
    val last30DaySalesOrderData = CampaignInput.loadLastNdaysOrderData(30, fullOrderData)

    val yesterdaySalesOrderItemData = CampaignInput.loadLastNdaysOrderItemData(1, fullOrderItemData) // created_at
    val yesterdaySalesOrderData = CampaignInput.loadLastNdaysOrderData(1, fullOrderData)

    val wishListCustomerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.WISH_LIST)

    val shortlistYesterdayData = CampaignInput.loadNthDayShortlistData(fullShortlistData, 1)
    val lastDayCustomerSelected = wishListCustomerSelector.customerSelection(shortlistYesterdayData)

    val shortlistLast30DayData = CampaignInput.loadNDaysShortlistData(fullShortlistData, 30)
    val last30DaysCustomerSelected = wishListCustomerSelector.customerSelection(shortlistLast30DayData)

    val itrSkuYesterdayData = CampaignInput.loadYesterdayItrSkuData()
    val itrSkuSimpleYesterdayData = CampaignInput.loadYesterdayItrSimpleData()

    val wishlistFollowupCampaign = new WishlistFollowupCampaign()
    wishlistFollowupCampaign.runCampaign(lastDayCustomerSelected, itrSkuYesterdayData, itrSkuSimpleYesterdayData, yesterdaySalesOrderData, yesterdaySalesOrderItemData)

    val past30DayCampaignMergedData = CampaignInput.load30DayCampaignMergedData()

    val wishListLowStockCampaign = new WishlistLowStockCampaign()
    wishListLowStockCampaign.runCampaign(past30DayCampaignMergedData, last30DaysCustomerSelected, itrSkuYesterdayData, itrSkuSimpleYesterdayData, last30DaySalesOrderData, last30DaySalesOrderItemData)

    // call iod campaign
    val itrSku30DayData = CampaignInput.load30DayItrSkuData()

    val wishListIODCampaign = new WishlistIODCampaign()
    wishListIODCampaign.runCampaign(past30DayCampaignMergedData, last30DaysCustomerSelected, itrSkuYesterdayData, itrSku30DayData, itrSkuSimpleYesterdayData, last30DaySalesOrderData, last30DaySalesOrderItemData)

  }

}
