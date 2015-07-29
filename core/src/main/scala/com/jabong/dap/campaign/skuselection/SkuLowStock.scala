package com.jabong.dap.campaign.skuselection

import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.variables._
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 *  Created by raghu on 26/7/15.
 * 2. Quantity of sku (SIMPLE- include size) falls is less than/equal to 10
 * 3. pick n ref based on special price (descending)
 * 4. This campaign should not have gone to the customer in the past 30 days for the same Ref SKU
 */
class SkuLowStock extends SkuSelector with Logging {

  override def skuFilter(dfCustomerPageVisit: DataFrame, dfItrData: DataFrame, dfCustomer: DataFrame, dfSalesOrder: DataFrame, dfSalesOrderItem: DataFrame): DataFrame = ???

  /**
   *
   * @param dfCustomerProductShortlist
   * @param dfYesterdaySkuItrData
   * @param dfYesterdaySkuSimpleItrData
   * @return
   */
  override def skuFilter(dfCustomerProductShortlist: DataFrame, dfYesterdaySkuItrData: DataFrame, dfYesterdaySkuSimpleItrData: DataFrame): DataFrame = {

    //=====================================calculate SKU data frame=====================================================
    val dfSkuLevel = shortListSkuFilter(dfCustomerProductShortlist, dfYesterdaySkuItrData)
      .withColumnRenamed(CustomerProductShortlistVariables.SKU, CustomerProductShortlistVariables.SKU_SIMPLE)
      .withColumnRenamed(CustomerProductShortlistVariables.AVERAGE_PRICE, CustomerProductShortlistVariables.SPECIAL_PRICE)

    //=========calculate SKU_SIMPLE data frame==========================================================================
    val dfSkuSimpleLevel = shortListSkuSimpleFilter(dfCustomerProductShortlist, dfYesterdaySkuSimpleItrData)

    //=======union both sku and sku simple==============================================================================
    val dfUnion = dfSkuLevel.unionAll(dfSkuSimpleLevel)

    //=========SKU_SIMPLE is mix of sku and sku-simple in case of shortlist======================================
    //=======select FK_CUSTOMER, EMAIL, SKU_SIMPLE, SPECIAL_PRICE=======================================================
    val dfResult = dfUnion.select(
      col(CustomerProductShortlistVariables.FK_CUSTOMER),
      col(CustomerProductShortlistVariables.EMAIL),
      col(CustomerProductShortlistVariables.SKU_SIMPLE) as ProductVariables.SKU_SIMPLE,
      col(CustomerProductShortlistVariables.SPECIAL_PRICE) as ProductVariables.SPECIAL_PRICE
    )

    return dfResult
  }

  //one day itr data
  //if average stock <= 10
  /**
   *
   * @param dfCustomerProductShortlist
   * @param dfItrData
   * @return
   */
  def shortListSkuFilter(dfCustomerProductShortlist: DataFrame, dfItrData: DataFrame): DataFrame = {

    if (dfCustomerProductShortlist == null || dfItrData == null) {

      logger.error("Data frame should not be null")

      return null

    }

    val skuSimpleCustomerProductShortlist = dfCustomerProductShortlist.select(
      CustomerProductShortlistVariables.FK_CUSTOMER,
      CustomerProductShortlistVariables.EMAIL,
      CustomerProductShortlistVariables.SKU
    )

    val itrData = dfItrData.select(
      col(ItrVariables.SKU) as ItrVariables.ITR_ + ItrVariables.SKU,
      col(ItrVariables.AVERAGE_STOCK),
      col(ItrVariables.AVERAGE_PRICE)
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
      col(CustomerProductShortlistVariables.SKU),
      col(CustomerProductShortlistVariables.AVERAGE_PRICE)
    )

    return dfResult
  }

  /**
   *
   * @param dfCustomerProductShortlist
   * @param dfItrData
   * @return
   */
  def shortListSkuSimpleFilter(dfCustomerProductShortlist: DataFrame, dfItrData: DataFrame): DataFrame = {

    if (dfCustomerProductShortlist == null || dfItrData == null) {

      logger.error("Data frame should not be null")

      return null

    }

    val skuSimpleCustomerProductShortlist = dfCustomerProductShortlist.select(
      CustomerProductShortlistVariables.FK_CUSTOMER,
      CustomerProductShortlistVariables.EMAIL,
      CustomerProductShortlistVariables.SKU_SIMPLE
    )

    val itrData = dfItrData.select(
      col(ItrVariables.SKU_SIMPLE) as ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE,
      col(ItrVariables.STOCK),
      col(ItrVariables.SPECIAL_PRICE)
    )

    val dfJoin = skuSimpleCustomerProductShortlist.join(
      itrData,
      skuSimpleCustomerProductShortlist(CustomerProductShortlistVariables.SKU_SIMPLE) === itrData(ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE),
      "inner"
    )

    val dfFilter = dfJoin.filter(ItrVariables.STOCK + " <= " + CampaignCommon.LOW_STOCK_VALUE)

    val dfResult = dfFilter.select(
      col(CustomerProductShortlistVariables.FK_CUSTOMER),
      col(CustomerProductShortlistVariables.EMAIL),
      col(CustomerProductShortlistVariables.SKU_SIMPLE),
      col(CustomerProductShortlistVariables.SPECIAL_PRICE)
    )

    return dfResult
  }

  override def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame, campaignName: String): DataFrame = ???

  override def skuFilter(inDataFrame: DataFrame): DataFrame = ???

  override def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame): DataFrame = ???
}
