package com.jabong.dap.campaign.customerselection

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.Daily._
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.{ CustomerSelection, CampaignCommon }
import com.jabong.dap.common.constants.variables._
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 30/9/15.
 */
class Last5SuccessfulOrder extends LiveCustomerSelector with Logging {

  override def customerSelection(customerData: DataFrame, fullSalesOrderData: DataFrame, fullSalesOrderItemData: DataFrame): DataFrame = {

    if (customerData == null || fullSalesOrderData == null || fullSalesOrderItemData == null) {

      logger.error("Data frame should not be null")

      return null
    }

    val filterCustomerData = customerData.filter(ContactListMobileVars.NET_ORDERS + " >= " + CampaignCommon.LAST_FIVE_PURCHASES)
      .select(CustomerVariables.ID_CUSTOMER)

    val joinedDf = filterCustomerData.join(
      fullSalesOrderData,
      filterCustomerData(CustomerVariables.ID_CUSTOMER) === fullSalesOrderData(SalesOrderVariables.FK_CUSTOMER),
      SQL.INNER
    )

    val lastOrder = new LastOrder()
    val dfCustomerSelection = lastOrder.customerSelection(joinedDf, fullSalesOrderItemData)

    dfCustomerSelection
  }

  override def customerSelection(inData: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, ndays: Int): DataFrame = ???

}
