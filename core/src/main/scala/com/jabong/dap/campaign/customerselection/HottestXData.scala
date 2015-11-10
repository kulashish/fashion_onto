package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.Utils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.variables.{CustomerVariables, ProductVariables, SalesOrderItemVariables, SalesOrderVariables}
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.model.order.variables.SalesOrderItem
import grizzled.slf4j.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


/**
 * Created by samathashetty on 2/11/15.
 */
class HottestXData extends CustomerSelector with Logging{
  override def customerSelection(inData: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(customerData: DataFrame, fullSalesOrderData: DataFrame, fullSalesOrderItemData: DataFrame): DataFrame =  {
    val day_45past = TimeUtils.getDateAfterNDays(-45, TimeConstants.DATE_FORMAT_FOLDER)

    val days_45_filter = Utils.getOneDayData(fullSalesOrderData, SalesOrderVariables.CREATED_AT, day_45past, TimeConstants.DATE_TIME_FORMAT)
      .filter(fullSalesOrderData(SalesOrderVariables.GW_AMOUNT).>(1000))

    val days_60_filter = Utils.getOneDayData(fullSalesOrderData, SalesOrderVariables.CREATED_AT, day_45past, TimeConstants.DATE_TIME_FORMAT)
      .filter(fullSalesOrderData(SalesOrderVariables.GW_AMOUNT).>(1000))

    val df_withOnly60 = days_60_filter.join(days_45_filter, days_45_filter(SalesOrderVariables.FK_CUSTOMER) === days_60_filter(SalesOrderVariables.FK_CUSTOMER),
      SQL.LEFT_OUTER)

    val days45_60Df = days_45_filter.unionAll(df_withOnly60)

    val filteredCustomerDf = customerData.join(days45_60Df,
      customerData(CustomerVariables.ID_CUSTOMER) === days45_60Df(SalesOrderVariables.FK_SALES_ORDER),
      SQL.RIGHT_OUTER)
      .select(SalesOrderVariables.FK_CUSTOMER, CustomerVariables.EMAIL, ProductVariables.SKU_SIMPLE)


    val lastOrder = new LastOrder()
    // WHy are we loading the full orders
    val dfCustomerSelection = lastOrder.customerSelection(fullSalesOrderData, fullSalesOrderItemData)

    val joinedDf = filteredCustomerDf.join(
      dfCustomerSelection,
      customerData(CustomerVariables.ID_CUSTOMER) === dfCustomerSelection(SalesOrderVariables.FK_SALES_ORDER),
      SQL.INNER
    ).select(
      SalesOrderVariables.FK_CUSTOMER,
      CustomerVariables.EMAIL,
      ProductVariables.SKU_SIMPLE
    )

    (joinedDf)

  }


  override def customerSelection(inData: DataFrame, inData2: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame): DataFrame = ???
}
