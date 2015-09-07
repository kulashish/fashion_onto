package com.jabong.dap.campaign.skuselection

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, SkuSelection }
import com.jabong.dap.common.constants.variables.{ ProductVariables, SalesOrderItemVariables, ItrVariables, CustomerProductShortlistVariables }
import com.jabong.dap.common.udf.Udf
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by rahul on 19/8/15.
 */
object Wishlist extends Logging {

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
  private def shortListSkuIODFilter(dfJoinCustomerWithYestardayItr: DataFrame, df30DaysItrData: DataFrame): DataFrame = {

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
