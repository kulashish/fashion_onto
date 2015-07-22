package com.jabong.dap.campaign.customerselection

import org.apache.spark.sql.DataFrame

/**
 * Surf1 - viewed same sku in the session
 * Surf2 - viewed 3 products from same brick in the session
 * Surf6 - viewed more than 5 distinct skus in the session
 *
 * Input - (user, session, deviceid, devicetype, [list of skus])
 */
class YesterdaySession extends CustomerSelector {

  override def customerSelection(inData: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, inData3: DataFrame): DataFrame = ???
}
