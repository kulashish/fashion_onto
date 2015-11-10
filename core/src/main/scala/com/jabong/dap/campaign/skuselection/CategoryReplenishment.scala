package com.jabong.dap.campaign.skuselection

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.variables.{ CustomerVariables, SalesOrderVariables, ProductVariables }
import com.jabong.dap.common.udf.Udf
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 30/9/15.
 */
object CategoryReplenishment extends Logging {

  def skuFilter(customerSkuData: DataFrame, yesterdayItrData: DataFrame): (DataFrame, DataFrame) = {

    if (customerSkuData == null || yesterdayItrData == null) {
      logger.error("either customer selected skus are null or itrData is null")
      return null
    }

    val filteredSkuJoinedItr = customerSkuData.join(yesterdayItrData, customerSkuData(ProductVariables.SKU_SIMPLE) === yesterdayItrData(ProductVariables.SKU_SIMPLE), SQL.INNER)
      .select(customerSkuData("*"),
        yesterdayItrData(ProductVariables.SPECIAL_PRICE),
        yesterdayItrData(ProductVariables.BRICK),
        yesterdayItrData(ProductVariables.BRAND),
        yesterdayItrData(ProductVariables.MVP),
        yesterdayItrData(ProductVariables.GENDER),
        yesterdayItrData(ProductVariables.PRODUCT_NAME),
        yesterdayItrData(ProductVariables.STOCK),
        yesterdayItrData(ProductVariables.PRICE_BAND),
        yesterdayItrData(ProductVariables.CATEGORY)
      )

    val filterNonBeauty = getNonBeauty(filteredSkuJoinedItr)

    val filterBeauty = getBeauty(filteredSkuJoinedItr)

    return (filterNonBeauty, filterBeauty)
  }

  def getNonBeauty(df: DataFrame): DataFrame = {

    val filterNonBeauty = df.filter(ProductVariables.CATEGORY + " != 'BEAUTY' and " + ProductVariables.CATEGORY + " ='FRAGRANCE'")
    val nonBeauty = filterNonBeauty.select(Udf.nonBeauty(filterNonBeauty(ProductVariables.CATEGORY), filterNonBeauty(SalesOrderVariables.CREATED_AT)) as ProductVariables.CATEGORY,
      filterNonBeauty(SalesOrderVariables.FK_CUSTOMER),
      filterNonBeauty(CustomerVariables.EMAIL),
      filterNonBeauty(ProductVariables.SKU_SIMPLE),
      filterNonBeauty(ProductVariables.SPECIAL_PRICE),
      filterNonBeauty(ProductVariables.BRICK),
      filterNonBeauty(ProductVariables.BRAND),
      filterNonBeauty(ProductVariables.MVP),
      filterNonBeauty(ProductVariables.GENDER),
      filterNonBeauty(ProductVariables.PRODUCT_NAME),
      filterNonBeauty(ProductVariables.STOCK))
      .na.drop("any", Array(ProductVariables.CATEGORY))

    nonBeauty
  }

  def getBeauty(df: DataFrame): DataFrame = {

    val filterBeauty = df.filter(ProductVariables.CATEGORY + " = 'BEAUTY' or " + ProductVariables.CATEGORY + " = 'FRAGRANCE'")
    val beauty = filterBeauty.select(Udf.beauty(filterBeauty(ProductVariables.CATEGORY), filterBeauty(SalesOrderVariables.CREATED_AT)) as ProductVariables.CATEGORY,
      filterBeauty(SalesOrderVariables.FK_CUSTOMER),
      filterBeauty(CustomerVariables.EMAIL),
      filterBeauty(ProductVariables.SKU_SIMPLE),
      filterBeauty(ProductVariables.SPECIAL_PRICE),
      filterBeauty(ProductVariables.BRICK),
      filterBeauty(ProductVariables.BRAND),
      filterBeauty(ProductVariables.MVP),
      filterBeauty(ProductVariables.GENDER),
      filterBeauty(ProductVariables.PRODUCT_NAME),
      filterBeauty(ProductVariables.STOCK))
      .na.drop("any", Array(ProductVariables.CATEGORY))

    beauty
  }

}
