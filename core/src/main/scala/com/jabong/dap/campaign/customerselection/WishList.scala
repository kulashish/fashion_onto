package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.constants.variables.CustomerWishlistVariables
import com.jabong.dap.common.schema.SchemaUtils
import com.jabong.dap.common.time.{ Constants, TimeUtils }
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Returns a list of customers based on following conditions:
 *      1. has added items to wishlist during last n days
 *
 *  Returns data in the following format:
 *    - list of [(id_customer, sku_simple, sku, updated_at(timestamp))]*
 */
class WishList extends LiveCustomerSelector {
  // not used
  override def customerSelection(wishlistData: DataFrame): DataFrame = ???

  // not used
  override def customerSelection(customerData: DataFrame, orderItemData: DataFrame): DataFrame = ???

  /**
   * This method will return DataFrame with column(fk_customer, sku_simple, sku, updated_at) from last n days
   * @param dfCustomerWishlist
   * @param ndays
   * @return DataFrame
   */
  override def customerSelection(dfCustomerWishlist: DataFrame, ndays: Int): DataFrame = {

    if (dfCustomerWishlist == null) {

      log("Data frame should not be null")

      return null

    }

    if (!SchemaUtils.isSchemaEqual(dfCustomerWishlist.schema, Schema.customerWishlist)) {

      log("schema attributes or data type mismatch")

      return null

    }

    val dateBeforeNdays = TimeUtils.getDateAfterNDays(-ndays, Constants.DATE_TIME_FORMAT)

    val dfResult = dfCustomerWishlist.filter(CustomerWishlistVariables.UPDATED_AT + ">=" + dateBeforeNdays)
      .select(
        col(CustomerWishlistVariables.FK_CUSTOMER),
        col(CustomerWishlistVariables.SIMPLE_SKU) as "sku_simple",
        col(CustomerWishlistVariables.CONFIGURABLE_SKU) as "sku",
        col(CustomerWishlistVariables.UPDATED_AT) as "updated_at")

    dfResult
  }
}
