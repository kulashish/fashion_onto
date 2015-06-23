package com.jabong.dap.campaign.common

import org.apache.spark.sql.DataFrame


/**
 * Created by jabong1145 on 15/6/15.
 */
trait Campaign {

  def customerSelection(customerData:DataFrame):DataFrame


  //def customerSkuFilter(customerData:DataFrame):DataFrame


}
