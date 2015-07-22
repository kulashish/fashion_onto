package com.jabong.dap.campaign.customerselection

import org.apache.spark.sql.DataFrame

/**
 * surf3 - viewed a sku yesterday and at least once during last 1-20 days (already available as input)
 */
class LastThirtyDaySession extends CustomerSelector {
  override def customerSelection(inData: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, inData3: DataFrame): DataFrame = ???
}
