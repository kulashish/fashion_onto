package com.jabong.dap.campaign.skuselection

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, SkuSelection }
import com.jabong.dap.common.constants.variables.{ ProductVariables, SalesOrderItemVariables, ItrVariables, CustomerVariables }
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
    val skuCustomerProductShortlist = dfCustomerProductShortlist.filter(CustomerVariables.SKU_SIMPLE + " is null or " + CustomerVariables.PRICE + " is null ")
      .select(
        CustomerVariables.FK_CUSTOMER,
        CustomerVariables.EMAIL,
        CustomerVariables.SKU,
        CustomerVariables.CREATED_AT
      )

    //val itrDay = lastDaySkuItrData.withColumnRenamed(ItrVariables.ITR_ + ItrVariables.AVERAGE_PRICE, CustomerVariables.SPECIAL_PRICE)

    val lastDaySkuItrData = dfLastDaySkuItrData.select(
      col(ItrVariables.SKU) as ItrVariables.ITR_ + ItrVariables.SKU,
      col(ItrVariables.SPECIAL_PRICE) as ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE,
      col(ItrVariables.STOCK) as ItrVariables.ITR_ + ItrVariables.STOCK,
      col(ItrVariables.CREATED_AT) as ItrVariables.ITR_ + ItrVariables.CREATED_AT,
      col(ItrVariables.BRAND) as ItrVariables.ITR_ + ItrVariables.BRAND,
      col(ItrVariables.BRICK) as ItrVariables.ITR_ + ItrVariables.BRICK,
      col(ItrVariables.MVP) as ItrVariables.ITR_ + ItrVariables.MVP,
      col(ItrVariables.GENDER) as ItrVariables.ITR_ + ItrVariables.GENDER,
      col(ProductVariables.PRODUCT_NAME) as ItrVariables.ITR_ + ProductVariables.PRODUCT_NAME
    )

    val joinDf = skuCustomerProductShortlist.join(lastDaySkuItrData, skuCustomerProductShortlist(CustomerVariables.SKU) === lastDaySkuItrData(ItrVariables.ITR_ + ItrVariables.SKU), SQL.INNER)
      .select(
        col(CustomerVariables.FK_CUSTOMER),
        col(CustomerVariables.EMAIL),
        col(CustomerVariables.SKU),
        col(ItrVariables.ITR_ + CustomerVariables.SPECIAL_PRICE) as CustomerVariables.SPECIAL_PRICE,
        col(ItrVariables.ITR_ + ItrVariables.STOCK) as ItrVariables.STOCK,
        col(ItrVariables.ITR_ + ItrVariables.CREATED_AT) as ItrVariables.CREATED_AT,
        col(CustomerVariables.CREATED_AT) as SalesOrderItemVariables.UPDATED_AT,
        col(ItrVariables.ITR_ + ItrVariables.BRAND) as ProductVariables.BRAND,
        col(ItrVariables.ITR_ + ItrVariables.BRICK) as ProductVariables.BRICK,
        col(ItrVariables.ITR_ + ItrVariables.MVP) as ProductVariables.MVP,
        col(ItrVariables.ITR_ + ItrVariables.GENDER) as ProductVariables.GENDER,
        col(ItrVariables.ITR_ + ProductVariables.PRODUCT_NAME) as ProductVariables.PRODUCT_NAME
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
    val skuSimpleCustomerProductShortlist = dfCustomerProductShortlist.filter(CustomerVariables.SKU_SIMPLE + " is not null and " + CustomerVariables.PRICE + " is not null ")
    var skuSimpleList = skuSimpleCustomerProductShortlist

    if (cType.equals(SkuSelection.ITEM_ON_DISCOUNT) || cType.equals(SkuSelection.LOW_STOCK)) {
      // join it with last day itr

      val yesterdayItrData = lastDayItrSimpleData.select(
        col(ItrVariables.SKU_SIMPLE) as ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE,
        col(ItrVariables.SPECIAL_PRICE) as ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE,
        col(ItrVariables.STOCK) as ItrVariables.ITR_ + ItrVariables.STOCK,
        col(ItrVariables.BRAND) as ItrVariables.ITR_ + ItrVariables.BRAND,
        col(ItrVariables.BRICK) as ItrVariables.ITR_ + ItrVariables.BRICK,
        col(ItrVariables.MVP) as ItrVariables.ITR_ + ItrVariables.MVP,
        col(ItrVariables.GENDER) as ItrVariables.ITR_ + ItrVariables.GENDER,
        col(ProductVariables.PRODUCT_NAME) as ItrVariables.ITR_ + ProductVariables.PRODUCT_NAME
      )
      val joinDF = skuSimpleCustomerProductShortlist.join(yesterdayItrData, skuSimpleCustomerProductShortlist(CustomerVariables.SKU_SIMPLE) === yesterdayItrData(ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE), "inner")
      var filteredDF: DataFrame = null

      if (cType.equals(SkuSelection.LOW_STOCK)) {
        filteredDF = joinDF.filter(ItrVariables.ITR_ + ItrVariables.STOCK + " <= " + CampaignCommon.LOW_STOCK_VALUE)
      } else {
        // iod
        filteredDF = joinDF.filter(CustomerVariables.PRICE + " > " + ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE)
      }

      skuSimpleList = filteredDF.select(
        col(CustomerVariables.FK_CUSTOMER),
        col(CustomerVariables.EMAIL),
        col(CustomerVariables.SKU_SIMPLE),
        col(CustomerVariables.CREATED_AT) as SalesOrderItemVariables.UPDATED_AT,
        col(ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE) as ItrVariables.SPECIAL_PRICE, // last day price
        col(ItrVariables.ITR_ + ItrVariables.STOCK) as ItrVariables.STOCK,
        col(ItrVariables.ITR_ + ItrVariables.BRAND) as ProductVariables.BRAND,
        col(ItrVariables.ITR_ + ItrVariables.BRICK) as ProductVariables.BRICK,
        col(ItrVariables.ITR_ + ItrVariables.MVP) as ProductVariables.MVP,
        col(ItrVariables.ITR_ + ItrVariables.GENDER) as ProductVariables.GENDER,
        col(ItrVariables.ITR_ + ProductVariables.PRODUCT_NAME) as ProductVariables.PRODUCT_NAME

      )
    } else {

      val filteredSkuJoinedItr = CampaignUtils.yesterdayItrJoin(skuSimpleCustomerProductShortlist, lastDayItrSimpleData)

      skuSimpleList = filteredSkuJoinedItr.select(
        col(CustomerVariables.FK_CUSTOMER),
        col(CustomerVariables.EMAIL),
        col(CustomerVariables.SKU_SIMPLE),
        col(CustomerVariables.CREATED_AT) as SalesOrderItemVariables.UPDATED_AT,
        col(CustomerVariables.PRICE) as ItrVariables.SPECIAL_PRICE,
        col(ProductVariables.BRAND),
        col(ProductVariables.BRICK),
        col(ProductVariables.MVP),
        col(ProductVariables.GENDER),
        col(ProductVariables.PRODUCT_NAME)
      )

    }

    val skuOnlyRecordsNotBought = CampaignUtils.skuSimpleNOTBought(skuSimpleList, orderData, orderItemData)

    // convert sku simple to sku
    val result = skuOnlyRecordsNotBought.select(
      col(CustomerVariables.FK_CUSTOMER),
      col(CustomerVariables.EMAIL),
      Udf.skuFromSimpleSku(dfCustomerProductShortlist(CustomerVariables.SKU_SIMPLE)) as CustomerVariables.SKU,
      col(CustomerVariables.SPECIAL_PRICE),
      col(ProductVariables.BRAND),
      col(ProductVariables.BRICK),
      col(ProductVariables.MVP),
      col(ProductVariables.GENDER),
      col(ProductVariables.PRODUCT_NAME)
    )
    result
  }

  // joinDF = already joined with last day itr
  // FIXME: average price
  private def shortListSkuIODFilter(dfJoinCustomerWithYestardayItr: DataFrame, df30DaysItrData: DataFrame): DataFrame = {

    val joinCustomerWithYestardayItr = dfJoinCustomerWithYestardayItr.select(
      col(CustomerVariables.FK_CUSTOMER),
      col(CustomerVariables.EMAIL),
      col(CustomerVariables.SKU),
      col(CustomerVariables.SPECIAL_PRICE),
      col(SalesOrderItemVariables.UPDATED_AT),
      Udf.yyyymmddString(dfJoinCustomerWithYestardayItr(CustomerVariables.CREATED_AT)) as CustomerVariables.CREATED_AT,
      col(ProductVariables.BRAND),
      col(ProductVariables.BRICK),
      col(ProductVariables.MVP),
      col(ProductVariables.GENDER),
      col(ProductVariables.PRODUCT_NAME)
    )

    val irt30Day = df30DaysItrData.select(
      col(ItrVariables.SKU) as ItrVariables.ITR_ + ItrVariables.SKU,
      col(ItrVariables.SPECIAL_PRICE) as ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE,
      Udf.yyyymmddString(df30DaysItrData(ItrVariables.CREATED_AT)) as ItrVariables.ITR_ + ItrVariables.CREATED_AT,
      col(ItrVariables.BRAND) as ItrVariables.ITR_ + ItrVariables.BRAND,
      col(ItrVariables.BRICK) as ItrVariables.ITR_ + ItrVariables.BRICK,
      col(ItrVariables.MVP) as ItrVariables.ITR_ + ItrVariables.MVP,
      col(ItrVariables.GENDER) as ItrVariables.ITR_ + ItrVariables.GENDER
    )

    val resultDf = joinCustomerWithYestardayItr.join(irt30Day, joinCustomerWithYestardayItr(CustomerVariables.SKU) === irt30Day(ItrVariables.ITR_ + ItrVariables.SKU)
      &&
      joinCustomerWithYestardayItr(CustomerVariables.CREATED_AT) === irt30Day(ItrVariables.ITR_ + ItrVariables.CREATED_AT), SQL.INNER)
      .filter(CustomerVariables.SPECIAL_PRICE + " > " + ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE)
      .select(
        CustomerVariables.FK_CUSTOMER,
        CustomerVariables.EMAIL,
        CustomerVariables.SKU,
        CustomerVariables.SPECIAL_PRICE,
        SalesOrderItemVariables.UPDATED_AT,
        ProductVariables.BRAND,
        ProductVariables.BRICK,
        ProductVariables.MVP,
        ProductVariables.GENDER,
        ProductVariables.PRODUCT_NAME
      )

    return resultDf

  }

}
