package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.variables.{ ItrVariables, CustomerPageVisitVariables }
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.storage.schema.Schema
import grizzled.slf4j.Logging
import org.apache.spark.sql.{ Row, DataFrame }
import org.apache.spark.sql.functions._
/**
 * Surf1 - viewed same sku in the actual_visit_id
 * Surf2 - viewed 3 products from same brick in the actual_visit_id
 *
 * Input - (user, actual_visit_id, brower_id, domain, [list of skus])
 */
class YesterdaySession extends CustomerSelector with Logging {

  /**
   * Surf1 - viewed same sku in the actual_visit_id
   * @param customerSurfData
   * @return
   */
  override def customerSelection(customerSurfData: DataFrame): DataFrame = {

    if (customerSurfData == null) {

      logger.error("Data frame should not be null")

      return null

    }
logger.info("schema of customerSurfData: " + customerSurfData.printSchema())

    val dfRepeatedSku = customerSurfData.select(
      col(CustomerPageVisitVariables.USER_ID),
      col(CustomerPageVisitVariables.BROWER_ID),
      col(CustomerPageVisitVariables.DOMAIN),
      explode(Udf.repeatedSku(col(CustomerPageVisitVariables.SKU_LIST))) as CustomerPageVisitVariables.SKU
    )

    return dfRepeatedSku
  }

  /**
   * Surf2 - viewed 3 products from same brick in the actual_visit_id
   * @param customerSurfData
   * @return
   */
  override def customerSelection(customerSurfData: DataFrame, dfYesterdayItrData: DataFrame): DataFrame = {

    if (customerSurfData == null || dfYesterdayItrData == null) {

      logger.error("Data frame should not be null")

      return null

    }

    val dfDistinctSku = customerSurfData.select(
      col(CustomerPageVisitVariables.USER_ID),
      col(CustomerPageVisitVariables.ACTUAL_VISIT_ID),
      col(CustomerPageVisitVariables.BROWER_ID),
      col(CustomerPageVisitVariables.DOMAIN),
      explode(Udf.distinctSku(col(CustomerPageVisitVariables.SKU_LIST))) as CustomerPageVisitVariables.SKU
    )

    val yesterdayItrData = dfYesterdayItrData.select(
      col(ItrVariables.SKU) as ItrVariables.ITR_ + ItrVariables.SKU,
      col(ItrVariables.BRICK)
    )

    val dfJoin = dfDistinctSku.join(
      yesterdayItrData,
      dfDistinctSku(CustomerPageVisitVariables.SKU) === yesterdayItrData(ItrVariables.ITR_ + ItrVariables.SKU),
      "inner"
    )
      .select(
        col(CustomerPageVisitVariables.USER_ID),
        col(CustomerPageVisitVariables.ACTUAL_VISIT_ID),
        col(ItrVariables.BRICK),
        col(CustomerPageVisitVariables.BROWER_ID),
        col(CustomerPageVisitVariables.DOMAIN),
        col(CustomerPageVisitVariables.SKU)
      )

    val rdd = dfJoin.map(row => ((row(0), row(1), row(2), row(3), row(4)) -> (Array(row(5)))))
      .reduceByKey(_ ++ _)
      .filter(_._2.length >= 3).map(row => Row(row._1._1, row._1._2, row._1._3, row._1._4, row._1._5, row._2))

    val dfResult = Spark.getSqlContext().createDataFrame(rdd, Schema.surf2)
      .select(
        col(CustomerPageVisitVariables.USER_ID),
        col(CustomerPageVisitVariables.BROWER_ID),
        col(CustomerPageVisitVariables.DOMAIN),
        explode(col(CustomerPageVisitVariables.SKU_LIST)) as CustomerPageVisitVariables.SKU
      )

    return dfResult
  }

  override def customerSelection(inData: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, inData3: DataFrame): DataFrame = ???
}
