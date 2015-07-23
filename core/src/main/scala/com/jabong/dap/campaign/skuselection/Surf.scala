package com.jabong.dap.campaign.skuselection

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.variables.{ CustomerVariables, CustomerPageVisitVariables, ItrVariables }
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
   * @param salesOrder
   * @param salesOrderItem
   * @return
   */
  def skuFilter(dfCustomerPageVisit: DataFrame, dfItrData: DataFrame, dfCustomer: DataFrame, salesOrder: DataFrame, salesOrderItem: DataFrame): DataFrame = {

    if (dfCustomerPageVisit == null || dfItrData == null) {

      logger.error("Data frame should not be null")

      return null

    }

    val skuCustomerPageVisit = dfCustomerPageVisit.select(
      CustomerPageVisitVariables.USER_ID,
      CustomerPageVisitVariables.PRODUCT_SKU,
      CustomerPageVisitVariables.BROWER_ID,
      CustomerPageVisitVariables.DOMAIN
    )

    val customer = dfCustomer.select(
      CustomerVariables.FK_CUSTOMER,
      CustomerVariables.EMAIL
    )

    //======= join data frame customer from skuCustomerPageVisit for mapping EMAIL to FK_CUSTOMER========
    val dfJoinCustomerToCustomerPageVisit = skuCustomerPageVisit.join(
      customer,
      skuCustomerPageVisit(CustomerPageVisitVariables.USER_ID) === customer(CustomerVariables.EMAIL),
      "left_outer"
    )
      .select(
        col(CustomerVariables.FK_CUSTOMER),
        col(CustomerPageVisitVariables.USER_ID) as CustomerVariables.EMAIL, // renaming for CampaignUtils.skuNotBought
        col(CustomerPageVisitVariables.PRODUCT_SKU),
        col(CustomerPageVisitVariables.BROWER_ID),
        col(CustomerPageVisitVariables.DOMAIN)
      )

    //=========calculate sku Not  Bought======================================================================
    val dfSkuNotBought = CampaignUtils.skuNotBought(dfJoinCustomerToCustomerPageVisit, salesOrder, salesOrderItem)

    val itrData = dfItrData.select(
      col(ItrVariables.SKU) as ItrVariables.ITR_ + ItrVariables.SKU,
      col(ItrVariables.AVERAGE_PRICE) as ItrVariables.SPECIAL_PRICE
    )

    //============== join data frame dfSkuNotBought from itrData===================================================
    val dfJoin = dfSkuNotBought.join(
      itrData,
      dfSkuNotBought(CustomerPageVisitVariables.PRODUCT_SKU) === itrData(ItrVariables.ITR_ + ItrVariables.SKU),
      "inner"
    )

    //===============generate reference SKU=============
    val dfReferenceSku = CampaignUtils.generateReferenceSku(dfJoin, 2)

    //===========select USER_ID,SKU, SPECIAL_PRICE================================================================
    val dfResult = dfReferenceSku.select(
      col(CustomerVariables.FK_CUSTOMER),
      col(CustomerVariables.EMAIL), //EMAIL can be encrypted EMAIL or BrowserId
      col(ItrVariables.SKU),
      col(ItrVariables.SPECIAL_PRICE)
    )

    return dfResult
  }

  override def skuFilter(inDataFrame: DataFrame): DataFrame = ???

  override def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame, campaignName: String): DataFrame = ???

  override def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame): DataFrame = ???
}
