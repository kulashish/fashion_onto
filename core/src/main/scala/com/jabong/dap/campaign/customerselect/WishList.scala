package com.jabong.dap.campaign.customerselect

import org.apache.spark.sql.DataFrame

/**
 * Created by jabong1145 on 7/7/15.
 */
class WishList extends LiveCustomerSelection{
  override def customerSelection(customerData: DataFrame): DataFrame = ???

  override def customerSelection(customerData: DataFrame, orderItemData: DataFrame): DataFrame = ???
}
