package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.{ SkuSelection, CampaignCommon, CustomerSelection }
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.common.udf.Udf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

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
    val lastDayCustomerSelected = wishListCustomerSelector.customerSelection(fullShortlistData, 1)

    val last30DaysCustomerSelected = wishListCustomerSelector.customerSelection(fullShortlistData, 30)

    val itrSkuYesterdayData = CampaignInput.loadYesterdayItrSkuData()
    val itrSkuSimpleYesterdayData = CampaignInput.loadYesterdayItrSimpleData()

    val wishlistFollowupCampaign = new WishlistFollowupCampaign()
    wishlistFollowupCampaign.runCampaign(lastDayCustomerSelected, itrSkuYesterdayData, itrSkuSimpleYesterdayData, yesterdaySalesOrderData, yesterdaySalesOrderItemData)

    val past30DayCampaignMergedData = CampaignInput.load30DayCampaignMergedData()

    val wishListLowStockCampaign = new WishlistLowStockCampaign()
    wishListLowStockCampaign.runCampaign(past30DayCampaignMergedData, fullShortlistData, itrSkuYesterdayData, itrSkuSimpleYesterdayData, last30DaySalesOrderData, last30DaySalesOrderItemData)

    // call iod campaign
    val itrSku30DayData = CampaignInput.load30DayItrSkuData()

    val wishListIODCampaign = new WishlistIODCampaign()
    wishListIODCampaign.runCampaign(past30DayCampaignMergedData, fullShortlistData, itrSkuYesterdayData, itrSku30DayData, itrSkuSimpleYesterdayData, last30DaySalesOrderData, last30DaySalesOrderItemData)

  }

}
