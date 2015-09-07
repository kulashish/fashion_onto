package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.constants.variables.PageVisitVariables
import com.jabong.dap.common.udf.Udf
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Surf6 - viewed more than 5 distinct skus in the actual_visit_id
 *
 * Input - (user, actual_visit_id, brower_id, domain, [list of skus])
 */
class YesterdaySessionDistinct extends CustomerSelector with Logging {

  /**
   * Surf6 - viewed more than 5 distinct skus in the actual_visit_id
   * @param customerSurfData
   * @return
   */
  override def customerSelection(customerSurfData: DataFrame): DataFrame = {

    if (customerSurfData == null) {

      logger.error("Data frame should not be null")

      return null

    }

    val dfDistinctSku = customerSurfData.select(
      col(PageVisitVariables.USER_ID),
      col(PageVisitVariables.ACTUAL_VISIT_ID),
      col(PageVisitVariables.BROWSER_ID),
      col(PageVisitVariables.DOMAIN),
      Udf.distinctList(col(PageVisitVariables.SKU_LIST)) as PageVisitVariables.SKU_LIST
    )

    val dfCountSku = dfDistinctSku.select(
      col(PageVisitVariables.USER_ID),
      col(PageVisitVariables.ACTUAL_VISIT_ID),
      col(PageVisitVariables.BROWSER_ID),
      col(PageVisitVariables.DOMAIN),
      col(PageVisitVariables.SKU_LIST),
      Udf.countSku(dfDistinctSku(PageVisitVariables.SKU_LIST)) as "count_sku"
    )

    val dfResult = dfCountSku.filter("count_sku >= " + 5)
      .select(
        col(PageVisitVariables.USER_ID),
        col(PageVisitVariables.BROWSER_ID),
        col(PageVisitVariables.DOMAIN),
        explode(col(PageVisitVariables.SKU_LIST)) as PageVisitVariables.SKU
      )

    return dfResult
  }

  override def customerSelection(inData: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, inData3: DataFrame): DataFrame = ???
}
