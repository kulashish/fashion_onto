package com.jabong.dap.campaign.skuselection

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.variables.{ ProductVariables, CustomerProductShortlistVariables, ItrVariables }
import com.jabong.dap.common.udf.Udf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class ShortlistFollowup {

  def skuFilter(dfCustomerProductShortlist: DataFrame, dfYesterdaySkuItrData: DataFrame, dfYesterdaySkuSimpleItrData: DataFrame): DataFrame = {

    //=====================================calculate SKU data frame=====================================================
    val itr30dayData = dfYesterdaySkuItrData.select(
      col(ItrVariables.SKU) as ItrVariables.ITR_ + ItrVariables.SKU,
      col(ItrVariables.AVERAGE_PRICE) as ItrVariables.ITR_ + ItrVariables.AVERAGE_PRICE,
      col(ItrVariables.CREATED_AT) as ItrVariables.ITR_ + ItrVariables.CREATED_AT
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
