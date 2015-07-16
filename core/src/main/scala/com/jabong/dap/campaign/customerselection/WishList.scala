package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.constants.variables.CustomerProductShortlistVariables
import com.jabong.dap.common.schema.SchemaUtils
import com.jabong.dap.common.time.{ Constants, TimeUtils }
import com.jabong.dap.data.storage.schema.Schema
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Returns a list of customers based on following conditions:
 *      1. has added items to wishlist during last n days
 *
 *  Returns data in the following format:
 *    - list of [(id_customer, sku_simple, sku, created_at(timestamp))]*
 */
class WishList extends LiveCustomerSelector with Logging {
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

      logger.error("Data frame should not be null")

      return null

    }

    if (ndays <= 0) {

      logger.error("ndays should not be negative value")

      return null

    }

    if (!SchemaUtils.isSchemaEqual(dfCustomerProductShortlist.schema, Schema.customerProductShortlist)) {

      logger.error("schema attributes or data type mismatch")

      return null

    }

    val dateBeforeNdays = TimeUtils.getDateAfterNDays(-ndays, Constants.DATE_TIME_FORMAT_MS)

    //FIXME: We are filtering cases where fk_customer is null and we have to check cases where fk_customer is null and email is not null
    val dfResult = dfCustomerProductShortlist.filter(CustomerProductShortlistVariables.FK_CUSTOMER + " is not null and " +
      //col(CustomerProductShortlist.EMAIL) + " is not null ) and " +
      CustomerProductShortlistVariables.REMOVED_AT + " is null and " +
      CustomerProductShortlistVariables.CREATED_AT + " >= " + "'" + dateBeforeNdays + "'")
      .select(
        col(CustomerProductShortlistVariables.FK_CUSTOMER),
        col(CustomerProductShortlistVariables.EMAIL),
        col(CustomerProductShortlistVariables.SKU),
        col(CustomerProductShortlistVariables.DOMAIN),
        col(CustomerProductShortlistVariables.USER_DEVICE_TYPE),
        col(CustomerProductShortlistVariables.CREATED_AT))

    dfResult
  }
}