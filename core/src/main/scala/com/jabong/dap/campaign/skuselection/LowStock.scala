package com.jabong.dap.campaign.skuselection

import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.variables.{ ItrVariables, CustomerProductShortlistVariables, CustomerVariables, ProductVariables }
import com.jabong.dap.common.schema.SchemaUtils
import com.jabong.dap.data.storage.schema.Schema
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * 2. Quantity of sku (SIMPLE- include size) falls is less than/equal to 10
 * 3. pick n ref based on special price (descending)
 * 4. This campaign should not have gone to the customer in the past 30 days for the same Ref SKU
 */
class LowStock extends SkuSelector with Logging {

  override def skuFilter(inDataFrame: DataFrame): DataFrame = ???

  // input will be [(id_customer, sku simple)] or [(id_customer, sku)]
  // case 1: only sku simple
  // case 2: only sku
  override def skuFilter(customerSkuData: DataFrame, itrDataFrame: DataFrame, campaignName: String): DataFrame = {
    if (customerSkuData == null || itrDataFrame == null || campaignName == null) {
      return null
    }
    var filteredSku: DataFrame = null
    if (campaignName == CampaignCommon.INVALID_CAMPAIGN) {
      filteredSku = customerSkuData.join(itrDataFrame, customerSkuData(ProductVariables.SKU) === itrDataFrame(ProductVariables.SKU), "inner")
        .filter(itrDataFrame(ProductVariables.STOCK + " <= " + CampaignCommon.LOW_STOCK_VALUE))
        .select(customerSkuData(CustomerVariables.FK_CUSTOMER), customerSkuData(ProductVariables.SKU), customerSkuData(ProductVariables.SPECIAL_PRICE))
    } else if (campaignName == CampaignCommon.WISHLIST_CAMPAIGN) {
      // separate sku and sku simples

    }

    return filteredSku
  }

  //one day itr data
  //if average stock <= 10
  def shortListSkuFilter(dfCustomerProductShortlist: DataFrame, dfItrData: DataFrame): DataFrame = {

    if (dfCustomerProductShortlist == null || dfItrData == null) {

      logger.error("Data frame should not be null")

      return null

    }

    if (!SchemaUtils.isSchemaEqual(dfCustomerProductShortlist.schema, Schema.resultCustomerProductShortlist) ||
      !SchemaUtils.isSchemaEqual(dfItrData.schema, Schema.itr)) {

      logger.error("schema attributes or data type mismatch")

      return null
    }
    val skuSimpleCustomerProductShortlist = dfCustomerProductShortlist.select(
      CustomerProductShortlistVariables.FK_CUSTOMER,
      CustomerProductShortlistVariables.EMAIL,
      CustomerProductShortlistVariables.SKU
    )

    val itrData = dfItrData.select(
      col(ItrVariables.SKU) as ItrVariables.ITR_ + ItrVariables.SKU,
      col(ItrVariables.AVERAGE_STOCK)
    )

    val dfJoin = skuSimpleCustomerProductShortlist.join(
      itrData,
      skuSimpleCustomerProductShortlist(CustomerProductShortlistVariables.SKU) === itrData(ItrVariables.ITR_ + ItrVariables.SKU),
      "inner"
    )

    val dfFilter = dfJoin.filter(ItrVariables.AVERAGE_STOCK + " <= " + CampaignCommon.LOW_STOCK_VALUE)

    val dfResult = dfFilter.select(
      col(CustomerProductShortlistVariables.FK_CUSTOMER),
      col(CustomerProductShortlistVariables.EMAIL),
      col(CustomerProductShortlistVariables.SKU)
    )

    return dfResult
  }

  def shortListSkuSimpleFilter(dfCustomerProductShortlist: DataFrame, dfItrData: DataFrame): DataFrame = {

    if (dfCustomerProductShortlist == null || dfItrData == null) {

      logger.error("Data frame should not be null")

      return null

    }

    if (!SchemaUtils.isSchemaEqual(dfCustomerProductShortlist.schema, Schema.resultCustomerProductShortlist) ||
      !SchemaUtils.isSchemaEqual(dfItrData.schema, Schema.itr)) {

      logger.error("schema attributes or data type mismatch")

      return null
    }
    val skuSimpleCustomerProductShortlist = dfCustomerProductShortlist.select(
      CustomerProductShortlistVariables.FK_CUSTOMER,
      CustomerProductShortlistVariables.EMAIL,
      CustomerProductShortlistVariables.SIMPLE_SKU
    )

    val itrData = dfItrData.select(
      col(ItrVariables.SIMPLE_SKU) as ItrVariables.ITR_ + ItrVariables.SIMPLE_SKU,
      col(ItrVariables.STOCK)
    )

    val dfJoin = skuSimpleCustomerProductShortlist.join(
      itrData,
      skuSimpleCustomerProductShortlist(CustomerProductShortlistVariables.SIMPLE_SKU) === itrData(ItrVariables.ITR_ + ItrVariables.SIMPLE_SKU),
      "inner"
    )

    val dfFilter = dfJoin.filter(ItrVariables.STOCK + " <= " + CampaignCommon.LOW_STOCK_VALUE)

    val dfResult = dfFilter.select(
      col(CustomerProductShortlistVariables.FK_CUSTOMER),
      col(CustomerProductShortlistVariables.EMAIL),
      col(CustomerProductShortlistVariables.SIMPLE_SKU)
    )

    return dfResult
  }

  override def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame): DataFrame = ???

}
