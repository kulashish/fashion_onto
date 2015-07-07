package com.jabong.dap.campaign.customerselect

import org.apache.spark.sql.DataFrame


/**
 * Created by jabong1145 on 15/6/15.
 */
trait CustomerSelection {

  def customerSelection(customerData:DataFrame):DataFrame


  def customerSelection(customerData:DataFrame,orderItemData:DataFrame):DataFrame


}
