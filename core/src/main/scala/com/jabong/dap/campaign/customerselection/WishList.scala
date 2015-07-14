package com.jabong.dap.campaign.customerselection

import org.apache.spark.sql.DataFrame

/**
 * Returns a list of customers based on following conditions:
 *      1. has added items to wishlist during last n days
 *
 *  Returns data in the following format:
 *    - list of [(id_customer, sku_simple, sku, timestamp)]*
 */
class WishList extends LiveCustomerSelector {
  // not used
  override def customerSelection(wishlistData: DataFrame): DataFrame = ???

  // not used
  override def customerSelection(customerData: DataFrame, orderItemData: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, ndays: Int): DataFrame = {
    null
  }
}
