package com.jabong.dap.campaign.customerselection

import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 11/9/15.
 */
class SalesOrder extends LiveCustomerSelector with Logging {

  override def customerSelection(salesOrder30DayData: DataFrame, salesOrderItemYesterdayData: DataFrame): DataFrame = {
    return null
  }

  override def customerSelection(inData: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, inData3: DataFrame): DataFrame = ???
}
