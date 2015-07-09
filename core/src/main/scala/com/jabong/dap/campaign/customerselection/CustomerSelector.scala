package com.jabong.dap.campaign.customerselection

import org.apache.spark.sql.DataFrame

/**
 * Customer selection interface - provides a dataframe of customers given input data
 *  - Input data will be campaign specific - e.g., abandoned cart data for acart campaign
 *    or order data for cancel retargeting etc.
 */
trait CustomerSelector {
  def customerSelection(inData: DataFrame): DataFrame
  def customerSelection(inData: DataFrame, ndays: Int): DataFrame
  def customerSelection(inData: DataFrame, inData2: DataFrame): DataFrame
}
