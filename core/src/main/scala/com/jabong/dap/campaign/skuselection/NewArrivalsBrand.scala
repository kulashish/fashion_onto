package com.jabong.dap.campaign.skuselection

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.variables.{ SalesCartVariables, CustomerVariables, ProductVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.model.product.itr.variables.ITR
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

    val dfItrGrouped = itrData.filter(ITR.ACTIVATED_AT + " > '" + yesterdayDate + "'")
      .groupBy(ITR.BRAND_NAME, ITR.GENDER).agg(count(ITR.BRAND_NAME) as "count", first(ITR.MVP) as ITR.MVP, first(ITR.SKU_SIMPLE) as ITR.SKU_SIMPLE)

    val dfItrFilteredSku = dfItrGrouped.filter(col("count").geq(4))
      .select(ITR.BRAND_NAME, ITR.GENDER, ITR.MVP, ITR.SKU_SIMPLE)

    val dfResult = customerSelected.join(dfItrFilteredSku, customerSelected(SalesCartVariables.SKU) === dfItrFilteredSku(ITR.SKU_SIMPLE), SQL.INNER)
      .select(SalesCartVariables.FK_CUSTOMER, SalesCartVariables.EMAIL, SalesCartVariables.SKU, ITR.BRAND_NAME, ITR.GENDER, ITR.MVP)

    return dfResult
  }

}
