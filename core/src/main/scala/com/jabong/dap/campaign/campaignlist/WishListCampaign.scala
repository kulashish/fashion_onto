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
    wishListLowStockCampaign.runCampaign(past30DayCampaignMergedData, last30DaysCustomerSelected, itrSkuYesterdayData, itrSkuSimpleYesterdayData, last30DaySalesOrderData, last30DaySalesOrderItemData)

    // call iod campaign
    val itrSku30DayData = CampaignInput.load30DayItrSkuData()

    val wishListIODCampaign = new WishlistIODCampaign()
    wishListIODCampaign.runCampaign(past30DayCampaignMergedData, last30DaysCustomerSelected, itrSkuYesterdayData, itrSku30DayData, itrSkuSimpleYesterdayData, last30DaySalesOrderData, last30DaySalesOrderItemData)

  }

  /**
   * *
   * @param dfCustomerProductShortlist
   * @param dfLastDaySkuItrData
   * @param last30daySkuItrData
   * @param orderData
   * @param orderItemData
   * @param cType
   * @return
   */
  def skuSelector(dfCustomerProductShortlist: DataFrame, dfLastDaySkuItrData: DataFrame, last30daySkuItrData: DataFrame, orderData: DataFrame, orderItemData: DataFrame, cType: String) = {
    val skuCustomerProductShortlist = dfCustomerProductShortlist.filter(CustomerProductShortlistVariables.SKU_SIMPLE + " is null or " + CustomerProductShortlistVariables.PRICE + " is null ")
      .select(
        CustomerProductShortlistVariables.FK_CUSTOMER,
        CustomerProductShortlistVariables.EMAIL,
        CustomerProductShortlistVariables.SKU,
        CustomerProductShortlistVariables.CREATED_AT
      )

    //val itrDay = lastDaySkuItrData.withColumnRenamed(ItrVariables.ITR_ + ItrVariables.AVERAGE_PRICE, CustomerProductShortlistVariables.SPECIAL_PRICE)

    val lastDaySkuItrData = dfLastDaySkuItrData.select(
      col(ItrVariables.SKU) as ItrVariables.ITR_ + ItrVariables.SKU,
      col(ItrVariables.SPECIAL_PRICE) as ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE,
      col(ItrVariables.STOCK) as ItrVariables.ITR_ + ItrVariables.STOCK,
      col(ItrVariables.CREATED_AT) as ItrVariables.ITR_ + ItrVariables.CREATED_AT
    )

    val joinDf = skuCustomerProductShortlist.join(lastDaySkuItrData, skuCustomerProductShortlist(CustomerProductShortlistVariables.SKU) === lastDaySkuItrData(ItrVariables.ITR_ + ItrVariables.SKU), SQL.INNER)
      .select(
        col(CustomerProductShortlistVariables.FK_CUSTOMER),
        col(CustomerProductShortlistVariables.EMAIL),
        col(CustomerProductShortlistVariables.SKU),
        col(ItrVariables.ITR_ + CustomerProductShortlistVariables.SPECIAL_PRICE) as CustomerProductShortlistVariables.SPECIAL_PRICE,
        col(ItrVariables.ITR_ + ItrVariables.STOCK) as ItrVariables.STOCK,
        col(ItrVariables.ITR_ + ItrVariables.CREATED_AT) as ItrVariables.CREATED_AT,
        col(CustomerProductShortlistVariables.CREATED_AT) as SalesOrderItemVariables.UPDATED_AT
      )

    var skuList = joinDf

    if (cType.equals(SkuSelection.LOW_STOCK)) {
      // FIXME: stock variable name
      skuList = joinDf.filter(ProductVariables.STOCK + " <= " + CampaignCommon.LOW_STOCK_VALUE)
    }

    if (cType.equals(SkuSelection.ITEM_ON_DISCOUNT)) {
      skuList = shortListSkuIODFilter(joinDf, last30daySkuItrData)
    }

    // FIXME: null replace, needs specialprice
    val skuOnlyRecordsNotBought = CampaignUtils.skuNotBought(skuList, orderData, orderItemData)

    skuOnlyRecordsNotBought

  }

  /**
   * *
   * @param dfCustomerProductShortlist
   * @param orderData      - 30 days for iod & lowstock, else pass only 1 day.
   * @param orderItemData  - 30 days for iod & lowstock, else pass only 1 day.
   * @param cType          - followup or iod or lowstock
   * @return
   */
  def skuSimpleSelector(dfCustomerProductShortlist: DataFrame, lastDayItrSimpleData: DataFrame, orderData: DataFrame, orderItemData: DataFrame, cType: String): DataFrame = {
    val skuSimpleCustomerProductShortlist = dfCustomerProductShortlist.filter(CustomerProductShortlistVariables.SKU_SIMPLE + " is not null and " + CustomerProductShortlistVariables.PRICE + " is not null ")
    var skuSimpleList = skuSimpleCustomerProductShortlist

    if (cType.equals(SkuSelection.ITEM_ON_DISCOUNT) || cType.equals(SkuSelection.LOW_STOCK)) {
      // join it with last day itr

      val yesterdayItrData = lastDayItrSimpleData.select(
        col(ItrVariables.SKU_SIMPLE) as ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE,
        col(ItrVariables.SPECIAL_PRICE) as ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE,
        col(ItrVariables.STOCK) as ItrVariables.ITR_ + ItrVariables.STOCK
      )
      val joinDF = skuSimpleCustomerProductShortlist.join(yesterdayItrData, skuSimpleCustomerProductShortlist(CustomerProductShortlistVariables.SKU_SIMPLE) === yesterdayItrData(ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE), "inner")
      var filteredDF: DataFrame = null

      if (cType.equals(SkuSelection.LOW_STOCK)) {
        filteredDF = joinDF.filter(ItrVariables.ITR_ + ItrVariables.STOCK + " <= " + CampaignCommon.LOW_STOCK_VALUE)
      } else {
        // iod
        filteredDF = joinDF.filter(CustomerProductShortlistVariables.PRICE + " > " + ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE)
      }

      skuSimpleList = filteredDF.select(
        col(CustomerProductShortlistVariables.FK_CUSTOMER),
        col(CustomerProductShortlistVariables.EMAIL),
        col(CustomerProductShortlistVariables.SKU_SIMPLE),
        col(CustomerProductShortlistVariables.CREATED_AT) as SalesOrderItemVariables.UPDATED_AT,
        col(ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE) as ItrVariables.SPECIAL_PRICE, // last day price
        col(ItrVariables.ITR_ + ItrVariables.STOCK) as ItrVariables.STOCK
      )
    } else {
      skuSimpleList = skuSimpleCustomerProductShortlist.select(
        col(CustomerProductShortlistVariables.FK_CUSTOMER),
        col(CustomerProductShortlistVariables.EMAIL),
        col(CustomerProductShortlistVariables.SKU_SIMPLE),
        col(CustomerProductShortlistVariables.CREATED_AT) as SalesOrderItemVariables.UPDATED_AT,
        col(CustomerProductShortlistVariables.PRICE) as ItrVariables.SPECIAL_PRICE
      )

    }

    val skuOnlyRecordsNotBought = CampaignUtils.skuSimpleNOTBought(skuSimpleList, orderData, orderItemData)

    // convert sku simple to sku
    val result = skuOnlyRecordsNotBought.select(
      col(CustomerProductShortlistVariables.FK_CUSTOMER),
      //col(CustomerProductShortlistVariables.EMAIL),
      Udf.skuFromSimpleSku(dfCustomerProductShortlist(CustomerProductShortlistVariables.SKU_SIMPLE)) as CustomerProductShortlistVariables.SKU,
      col(CustomerProductShortlistVariables.SPECIAL_PRICE)
    )
    result
  }

  // joinDF = already joined with last day itr
  // FIXME: average price
  def shortListSkuIODFilter(dfJoinCustomerWithYestardayItr: DataFrame, df30DaysItrData: DataFrame): DataFrame = {

    val joinCustomerWithYestardayItr = dfJoinCustomerWithYestardayItr.select(
      col(CustomerProductShortlistVariables.FK_CUSTOMER),
      col(CustomerProductShortlistVariables.EMAIL),
      col(CustomerProductShortlistVariables.SKU),
      col(CustomerProductShortlistVariables.SPECIAL_PRICE),
      col(SalesOrderItemVariables.UPDATED_AT),
      Udf.yyyymmddString(dfJoinCustomerWithYestardayItr(CustomerProductShortlistVariables.CREATED_AT)) as CustomerProductShortlistVariables.CREATED_AT
    )

    val irt30Day = df30DaysItrData.select(
      col(ItrVariables.SKU) as ItrVariables.ITR_ + ItrVariables.SKU,
      col(ItrVariables.SPECIAL_PRICE) as ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE,
      Udf.yyyymmddString(df30DaysItrData(ItrVariables.CREATED_AT)) as ItrVariables.ITR_ + ItrVariables.CREATED_AT
    )

    val resultDf = joinCustomerWithYestardayItr.join(irt30Day, joinCustomerWithYestardayItr(CustomerProductShortlistVariables.SKU) === irt30Day(ItrVariables.ITR_ + ItrVariables.SKU)
      &&
      joinCustomerWithYestardayItr(CustomerProductShortlistVariables.CREATED_AT) === irt30Day(ItrVariables.ITR_ + ItrVariables.CREATED_AT), SQL.INNER)
      .filter(CustomerProductShortlistVariables.SPECIAL_PRICE + " > " + ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE)
      .select(
        CustomerProductShortlistVariables.FK_CUSTOMER,
        CustomerProductShortlistVariables.EMAIL,
        CustomerProductShortlistVariables.SKU,
        CustomerProductShortlistVariables.SPECIAL_PRICE,
        SalesOrderItemVariables.UPDATED_AT
      )

    return resultDf

  }

}
