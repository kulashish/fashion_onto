package com.jabong.dap.campaign.skuselection

import com.jabong.dap.campaign.traceability.PastCampaignCheck
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.variables.{ ProductVariables, CustomerVariables, CustomerPageVisitVariables, ItrVariables }
import com.jabong.dap.model.product.itr.variables.ITR
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
  override def skuFilter(past30DayCampaignMergedData: DataFrame, dfCustomerPageVisit: DataFrame, dfItrData: DataFrame, dfCustomer: DataFrame, dfSalesOrder: DataFrame, dfSalesOrderItem: DataFrame, campaignName: String): DataFrame = {

    if (dfCustomerPageVisit == null || dfItrData == null || dfCustomer == null || dfSalesOrder == null || dfSalesOrderItem == null) {

      logger.error("Data frame should not be null")

      return null

    }

    val dfCustomerEmailToCustomerId = CampaignUtils.getMappingCustomerEmailToCustomerId(dfCustomerPageVisit, dfCustomer)

    val itrData = dfItrData.select(
      col(ItrVariables.SKU) as ItrVariables.ITR_ + ItrVariables.SKU,
      col(ITR.SPECIAL_PRICE) as ProductVariables.SPECIAL_PRICE
    )

    val dfSkuNotBought = CampaignUtils.skuNotBoughtR2(dfCustomerEmailToCustomerId, dfSalesOrder, dfSalesOrderItem).
      withColumnRenamed(ItrVariables.SKU, ProductVariables.SKU_SIMPLE)

    var skusFiltered = dfSkuNotBought
    if (past30DayCampaignMergedData != null) {
      //past campaign check whether the campaign has been sent to customer in last 30 days
      val pastCampaignCheck = new PastCampaignCheck()
      skusFiltered = pastCampaignCheck.campaignRefSkuCheck(past30DayCampaignMergedData, dfSkuNotBought,
        CampaignCommon.campaignMailTypeMap.getOrElse(campaignName, 1000), 30)
    }

    //FIXME: when it is fixed in customer master records
    val toLong    = udf[Long, String]( _.toLong)

    val dfJoin = skusFiltered.join(
      itrData,
      skusFiltered(ProductVariables.SKU_SIMPLE) === itrData(ItrVariables.ITR_ + ItrVariables.SKU),
      "inner"
    )
      .select(
        col(CustomerVariables.FK_CUSTOMER),
        col(CustomerVariables.EMAIL), //EMAIL can be encrypted EMAIL or BrowserId
        col(ProductVariables.SKU_SIMPLE),
        col(ProductVariables.SPECIAL_PRICE),
        col(CustomerPageVisitVariables.BROWER_ID),
        col(CustomerPageVisitVariables.DOMAIN)
      )

    return dfJoin
  }

  override def skuFilter(inDataFrame: DataFrame): DataFrame = ???

  override def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame, campaignName: String): DataFrame = ???

  override def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame): DataFrame = ???

  override def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame, inDataFrame3: DataFrame): DataFrame = ???

  override def skuFilter(dfCustomerPageVisit: DataFrame, dfItrData: DataFrame, dfCustomer: DataFrame, dfSalesOrder: DataFrame, dfSalesOrderItem: DataFrame): DataFrame = ???
}
