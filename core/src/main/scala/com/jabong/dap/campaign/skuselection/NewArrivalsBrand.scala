package com.jabong.dap.campaign.skuselection

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.variables.{ SalesCartVariables, ProductVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
/**
 * Created by raghu on 7/9/15.
 */
object NewArrivalsBrand extends Logging {

  def skuFilter(customerSelected: DataFrame, itrData: DataFrame): DataFrame = {

    if (customerSelected == null || itrData == null) {

      logger.error("Data frame should not be null")

      return null

    }

    val yesterdayDateTime = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_TIME_FORMAT)
    val yesterdayDate = yesterdayDateTime.substring(0, yesterdayDateTime.indexOf(" ") + 1) + TimeConstants.START_TIME

    val dfItrGrouped = itrData.filter(ProductVariables.ACTIVATED_AT + " > '" + yesterdayDate + "'")
      .groupBy(ProductVariables.BRAND, ProductVariables.GENDER).agg(count(ProductVariables.BRAND) as "count", first(ProductVariables.SPECIAL_PRICE) as ProductVariables.SPECIAL_PRICE, first(ProductVariables.BRICK) as ProductVariables.BRICK, first(ProductVariables.MVP) as ProductVariables.MVP, first(ProductVariables.PRODUCT_NAME) as ProductVariables.PRODUCT_NAME, first(ProductVariables.SKU_SIMPLE) as ProductVariables.SKU_SIMPLE)

    val dfItrFilteredSku = dfItrGrouped.filter(col("count").geq(CampaignCommon.COUNT_NEW_ARRIVALS))
      .select(ProductVariables.BRAND, ProductVariables.BRICK, ProductVariables.GENDER, ProductVariables.PRODUCT_NAME, ProductVariables.MVP, ProductVariables.SKU_SIMPLE, ProductVariables.SPECIAL_PRICE)

    val dfResult = customerSelected.join(dfItrFilteredSku, customerSelected(SalesCartVariables.SKU) === dfItrFilteredSku(ProductVariables.SKU_SIMPLE), SQL.INNER)
      .select(
        col(SalesCartVariables.FK_CUSTOMER),
        col(SalesCartVariables.EMAIL),
        col(SalesCartVariables.SKU) as ProductVariables.SKU_SIMPLE,
        col(ProductVariables.SPECIAL_PRICE),
        col(ProductVariables.BRAND),
        col(ProductVariables.BRICK),
        col(ProductVariables.GENDER),
        col(ProductVariables.MVP),
        col(ProductVariables.PRODUCT_NAME)
      )

    return dfResult
  }

}
