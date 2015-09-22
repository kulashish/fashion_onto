package com.jabong.dap.campaign.skuselection

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.common.udf.Udf
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object FollowUp extends Logging {

  // 1. stock should be >= 10
  // 2. pick based on special price (descending)
  // 1 day data
  // inDataFrame =  [(id_customer, sku simple)]
  // itrData = [(skusimple, date, stock, special price)]
  def skuFilter(customerSkuData: DataFrame, itrData: DataFrame): DataFrame = {
    if (customerSkuData == null || itrData == null) {
      logger.error("either customer selected skus are null or itrData is null")
      return null
    }

    val filteredSku = customerSkuData.join(itrData, customerSkuData(ProductVariables.SKU_SIMPLE) === itrData(ProductVariables.SKU_SIMPLE), SQL.INNER)
      .filter(ProductVariables.STOCK + " >= " + CampaignCommon.FOLLOW_UP_STOCK_VALUE)
      .select(customerSkuData(CustomerVariables.FK_CUSTOMER),
        customerSkuData(ProductVariables.SKU_SIMPLE),
        itrData(ProductVariables.SPECIAL_PRICE))

    logger.info("Join selected customer sku with sku data and filter by stock>=" + CampaignCommon.FOLLOW_UP_STOCK_VALUE)
    //generate reference skus
    //    val refSkus = CampaignUtils.generateReferenceSku(filteredSku, CampaignCommon.NUMBER_REF_SKUS)

    return filteredSku
  }

  def skuFilterblahBlah(dfCustomerProductShortlist: DataFrame, df30DaysSkuItrData: DataFrame, dfYesterdaySkuSimpleItrData: DataFrame): DataFrame = {
    //=====================================calculate SKU data frame=====================================================
    val itr30dayData = df30DaysSkuItrData.select(
      col(ItrVariables.SKU) as ItrVariables.ITR_ + ItrVariables.SKU,
      col(ItrVariables.AVERAGE_PRICE) as ItrVariables.ITR_ + ItrVariables.AVERAGE_PRICE,
      Udf.yyyymmdd(df30DaysSkuItrData(ItrVariables.CREATED_AT)) as ItrVariables.ITR_ + ItrVariables.CREATED_AT
    )

    //========df30DaysSkuItrData filter for yesterday===================================================================
    val dfYesterdayItrData = CampaignUtils.getYesterdayItrData(itr30dayData)

    val dfSkuLevel = CampaignUtils.shortListSkuItrJoin(dfCustomerProductShortlist, dfYesterdayItrData, itr30dayData)
      .select(
        CustomerProductShortlistVariables.FK_CUSTOMER,
        CustomerProductShortlistVariables.EMAIL,
        CustomerProductShortlistVariables.SKU,
        CustomerProductShortlistVariables.AVERAGE_PRICE
      )

      .withColumnRenamed(CustomerProductShortlistVariables.SKU, CustomerProductShortlistVariables.SKU_SIMPLE)
      .withColumnRenamed(CustomerProductShortlistVariables.AVERAGE_PRICE, CustomerProductShortlistVariables.SPECIAL_PRICE)

    //=========calculate SKU_SIMPLE data frame==========================================================================
    val yesterdaySkuSimpleItrData = dfYesterdaySkuSimpleItrData.select(
      col(ItrVariables.SKU_SIMPLE) as ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE,
      col(ItrVariables.SPECIAL_PRICE) as ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE
    )
    val dfSkuSimpleLevel = CampaignUtils.shortListSkuSimpleItrJoin(dfCustomerProductShortlist, yesterdaySkuSimpleItrData)
      .select(
        col(CustomerProductShortlistVariables.FK_CUSTOMER),
        col(CustomerProductShortlistVariables.EMAIL),
        col(CustomerProductShortlistVariables.SKU_SIMPLE),
        col(CustomerProductShortlistVariables.PRICE) as CustomerProductShortlistVariables.SPECIAL_PRICE
      )
    //=======union both sku and sku simple==============================================================================
    val dfUnion = dfSkuLevel.unionAll(dfSkuSimpleLevel)

    //=========SKU_SIMPLE is mix of sku and sku-simple in case of shortlist======================================
    //=======select FK_CUSTOMER, EMAIL, SKU_SIMPLE, SPECIAL_PRICE=======================================================
    val dfResult = dfUnion.select(col(CustomerProductShortlistVariables.FK_CUSTOMER),
      col(CustomerProductShortlistVariables.EMAIL),
      col(CustomerProductShortlistVariables.SKU_SIMPLE) as ProductVariables.SKU_SIMPLE,
      col(CustomerProductShortlistVariables.SPECIAL_PRICE) as ProductVariables.SPECIAL_PRICE
    )

    return dfResult
  }
}
