package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.constants.variables.CustomerProductShortlist
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
 *    - list of [(id_customer, sku_simple, sku, created_at(timestamp))]*
 */
class WishList extends LiveCustomerSelector {
  // not used
  override def customerSelection(wishlistData: DataFrame): DataFrame = ???

  // not used
  override def customerSelection(customerData: DataFrame, orderItemData: DataFrame): DataFrame = ???

  /**
   * This method will return DataFrame with column(fk_customer, sku, domain, user_device_type, created_at) from last n days
   * @param dfCustomerProductShortlist
   * @param ndays
   * @return DataFrame
   */
  override def customerSelection(dfCustomerProductShortlist: DataFrame, ndays: Int): DataFrame = {

    if (dfCustomerProductShortlist == null) {

      log("Data frame should not be null")

      return null

    }

    if (ndays <= 0) {

      log("ndays should not be negative value")

      return null

    }

    if (!SchemaUtils.isSchemaEqual(dfCustomerProductShortlist.schema, Schema.customerProductShortlist)) {

      log("schema attributes or data type mismatch")

      return null

    }

    val dateBeforeNdays = TimeUtils.getDateAfterNDays(-ndays, Constants.DATE_TIME_FORMAT_MS)

    //FIXME: We are filtering cases where fk_customer is null and we have to check cases where fk_customer is null and email is not null
    val dfResult = dfCustomerProductShortlist.filter(CustomerProductShortlist.FK_CUSTOMER + " is not null and " +
      //col(CustomerProductShortlist.EMAIL) + " is not null ) and " +
      CustomerProductShortlist.REMOVED_AT + " is null and " +
      CustomerProductShortlist.CREATED_AT + " >= " + "'" + dateBeforeNdays + "'")
      .select(
        col(CustomerProductShortlist.FK_CUSTOMER),
        col(CustomerProductShortlist.EMAIL),
        col(CustomerProductShortlist.SKU),
        col(CustomerProductShortlist.DOMAIN),
        col(CustomerProductShortlist.USER_DEVICE_TYPE),
        col(CustomerProductShortlist.CREATED_AT))

    dfResult
  }

  override def customerSelection(inData: DataFrame, inData2: DataFrame, ndays: Int): DataFrame = ???

}
