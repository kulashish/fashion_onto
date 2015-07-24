package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.variables.{ ItrVariables, CustomerPageVisitVariables }
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.{ Row, DataFrame }
import org.apache.spark.sql.functions._
/**
 * Surf1 - viewed same sku in the actual_visit_id
 * Surf2 - viewed 3 products from same brick in the actual_visit_id
 * Surf6 - viewed more than 5 distinct skus in the actual_visit_id
 *
 * Input - (user, actual_visit_id, brower_id, domain, [list of skus])
 */
class YesterdaySession extends CustomerSelector {

  /**
   * Surf1 - viewed same sku in the actual_visit_id
   * @param customerSurfData
   * @return
   */
  override def customerSelection(customerSurfData: DataFrame): DataFrame = {

    val dfRepeatedSku = customerSurfData.select(
      col(CustomerPageVisitVariables.USER_ID),
      col(CustomerPageVisitVariables.BROWER_ID),
      col(CustomerPageVisitVariables.DOMAIN),
      explode(Udf.repeatedSku(col(CustomerPageVisitVariables.SKU_LIST))) as CustomerPageVisitVariables.PRODUCT_SKU
    )

    return dfRepeatedSku
  }

  /**
   * Surf2 - viewed 3 products from same brick in the actual_visit_id
   * @param customerSurfData
   * @return
   */
  override def customerSelection(customerSurfData: DataFrame, dfYesterdayItrData: DataFrame): DataFrame = {

    val dfDistinctSku = customerSurfData.select(
      col(CustomerPageVisitVariables.USER_ID),
      col(CustomerPageVisitVariables.ACTUAL_VISIT_ID),
      col(CustomerPageVisitVariables.BROWER_ID),
      col(CustomerPageVisitVariables.DOMAIN),
      explode(Udf.distinctSku(col(CustomerPageVisitVariables.SKU_LIST))) as CustomerPageVisitVariables.PRODUCT_SKU
    )

    val yesterdayItrData = dfYesterdayItrData.select(
      ItrVariables.SKU,
      ItrVariables.BRICK
    )
    val dfJoin = dfDistinctSku.join(
      yesterdayItrData,
      dfDistinctSku(CustomerPageVisitVariables.PRODUCT_SKU) === yesterdayItrData(ItrVariables.SKU),
      "inner"
    )
      .select(
        col(CustomerPageVisitVariables.USER_ID),
        col(CustomerPageVisitVariables.ACTUAL_VISIT_ID),
        col(ItrVariables.BRICK),
        col(CustomerPageVisitVariables.BROWER_ID),
        col(CustomerPageVisitVariables.DOMAIN),
        col(CustomerPageVisitVariables.PRODUCT_SKU)
      )

    val rdd = dfJoin.map(row => ((row(0), row(1), row(2), row(3), row(4)) -> (Array(row(5)))))
      .reduceByKey(_ ++ _)
      .filter(_._2.length >= 3).map(row => Row(row._1._1, row._1._2, row._1._3, row._1._4, row._1._5, row._2))

    val dfResult = Spark.getSqlContext().createDataFrame(rdd, Schema.surf2)
      .select(
        col(CustomerPageVisitVariables.USER_ID),
        col(CustomerPageVisitVariables.BROWER_ID),
        col(CustomerPageVisitVariables.DOMAIN),
        explode(col(CustomerPageVisitVariables.SKU_LIST)) as CustomerPageVisitVariables.PRODUCT_SKU
      )

    return dfResult
  }

  /**
   * Surf6 - viewed more than 5 distinct skus in the actual_visit_id
   * @param customerSurfData
   * @return
   */
  def customerSelectionSurf6(customerSurfData: DataFrame): DataFrame = {

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

  override def customerSelection(inData: DataFrame, inData2: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, inData3: DataFrame): DataFrame = ???
}
