package com.jabong.dap.campaign.skuselection

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.variables.{ ProductVariables, CustomerVariables, CustomerPageVisitVariables, ItrVariables }
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * two options:
 * 1. surf 1, 6, 2 - group by user
 * 2  surf 3
 *
 * 1. not bought yesterday
 * 2.past campaign check
 * 3. ref skus based on special price descending
 */
class Surf extends SkuSelector with Logging {

  /**
   * surf 3
   * @param dfCustomerPageVisit
   * @param dfItrData
   * @param dfCustomer
   * @param dfSalesOrder
   * @param dfSalesOrderItem
   * @return
   */
   def skuFilter(dfCustomerPageVisit: DataFrame, dfItrData: DataFrame, dfCustomer: DataFrame, dfSalesOrder: DataFrame, dfSalesOrderItem: DataFrame): DataFrame = {

    if (dfCustomerPageVisit == null || dfItrData == null || dfCustomer == null || dfSalesOrder == null || dfSalesOrderItem == null) {

      logger.error("Data frame should not be null")

      return null

    }

    val itrData = dfItrData.select(
      col(ItrVariables.SKU) as ItrVariables.ITR_ + ItrVariables.SKU,
      col(ItrVariables.AVERAGE_PRICE) as ProductVariables.SPECIAL_PRICE
    )

    val dfCustomerEmailToCustomerId = CampaignUtils.getMappingCustomerEmailToCustomerId(dfCustomerPageVisit, dfCustomer)

    val dfSkuNotBought = CampaignUtils.skuNotBought(dfCustomerEmailToCustomerId, dfSalesOrder, dfSalesOrderItem)

    val dfJoin = dfSkuNotBought.join(
      itrData,
      dfSkuNotBought(CustomerPageVisitVariables.SKU) === itrData(ItrVariables.ITR_ + ItrVariables.SKU),
      "inner"
    )
      .select(
        col(CustomerVariables.FK_CUSTOMER),
        col(CustomerVariables.EMAIL), //EMAIL can be encrypted EMAIL or BrowserId
        col(ItrVariables.SKU) as ProductVariables.SKU_SIMPLE,
        col(ProductVariables.SPECIAL_PRICE)
      )

    //FIXME: generate Reference Skus
    //    val dfReferenceSku = CampaignUtils.generateReferenceSkus(dfJoin, 2)

    return dfJoin
  }

  override def skuFilter(inDataFrame: DataFrame): DataFrame = ???

  override def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame, campaignName: String): DataFrame = ???

  override def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame): DataFrame = ???

  override def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame, inDataFrame3: DataFrame): DataFrame = ???
}
