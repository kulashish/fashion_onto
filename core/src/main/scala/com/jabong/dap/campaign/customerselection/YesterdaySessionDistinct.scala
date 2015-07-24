package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.constants.variables.CustomerPageVisitVariables
import com.jabong.dap.common.udf.Udf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Surf6 - viewed more than 5 distinct skus in the actual_visit_id
 *
 * Input - (user, actual_visit_id, brower_id, domain, [list of skus])
 */
class YesterdaySessionDistinct extends CustomerSelector {

  /**
   * Surf6 - viewed more than 5 distinct skus in the actual_visit_id
   * @param customerSurfData
   * @return
   */
  override def customerSelection(customerSurfData: DataFrame): DataFrame = {

    val dfDistinctSku = customerSurfData.select(
      col(CustomerPageVisitVariables.USER_ID),
      col(CustomerPageVisitVariables.ACTUAL_VISIT_ID),
      col(CustomerPageVisitVariables.BROWER_ID),
      col(CustomerPageVisitVariables.DOMAIN),
      Udf.distinctSku(col(CustomerPageVisitVariables.SKU_LIST)) as CustomerPageVisitVariables.SKU_LIST
    )

    val dfCountSku = dfDistinctSku.select(
      col(CustomerPageVisitVariables.USER_ID),
      col(CustomerPageVisitVariables.ACTUAL_VISIT_ID),
      col(CustomerPageVisitVariables.BROWER_ID),
      col(CustomerPageVisitVariables.DOMAIN),
      col(CustomerPageVisitVariables.SKU_LIST),
      Udf.countSku(dfDistinctSku(CustomerPageVisitVariables.SKU_LIST)) as CustomerPageVisitVariables.COUNT_SKU
    )

    val dfResult = dfCountSku.filter(CustomerPageVisitVariables.COUNT_SKU + " >= " + 5)
      .select(
        col(CustomerPageVisitVariables.USER_ID),
        col(CustomerPageVisitVariables.BROWER_ID),
        col(CustomerPageVisitVariables.DOMAIN),
        explode(col(CustomerPageVisitVariables.SKU_LIST)) as CustomerPageVisitVariables.PRODUCT_SKU
      )

    return dfResult
  }

  override def customerSelection(inData: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, inData3: DataFrame): DataFrame = ???
}
