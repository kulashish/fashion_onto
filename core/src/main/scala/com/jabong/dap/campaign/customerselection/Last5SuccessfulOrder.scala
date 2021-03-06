package com.jabong.dap.campaign.customerselection

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.variables._
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 30/9/15.
 */
class Last5SuccessfulOrder extends LiveCustomerSelector with Logging {

  override def customerSelection(customerOrderData: DataFrame, salesOrderData: DataFrame, salesOrderItemData: DataFrame): DataFrame = {

    if (customerOrderData == null || salesOrderData == null || salesOrderItemData == null) {

      logger.error("Data frame should not be null")

      return null
    }
    CampaignUtils.debug(customerOrderData, "customerOrder Data")

    val filterCustomerData = customerOrderData.filter(SalesOrderItemVariables.SUCCESSFUL_ORDERS + " >= " + CampaignCommon.LAST_FIVE_PURCHASES)
      .select(CustomerVariables.FK_CUSTOMER)

    CampaignUtils.debug(filterCustomerData, "filterCustomerData with 5 successfull orders")

    val coalesceFullSalesOrderData = salesOrderData
    val joinedDf = filterCustomerData.join(
      coalesceFullSalesOrderData,
      filterCustomerData(CustomerVariables.FK_CUSTOMER) === coalesceFullSalesOrderData(SalesOrderVariables.FK_CUSTOMER),
      SQL.INNER
    ).select(coalesceFullSalesOrderData("*"))

    CampaignUtils.debug(joinedDf, "last 5 orders joinedDf")

    val lastOrder = new LastOrder()
    val dfCustomerSelection = lastOrder.customerSelection(joinedDf, salesOrderItemData)

    CampaignUtils.debug(dfCustomerSelection, "last 5 orders dfCustomerSelection")

    dfCustomerSelection
  }

  override def customerSelection(inData: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, ndays: Int): DataFrame = ???

}
