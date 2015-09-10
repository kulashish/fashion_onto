package com.jabong.dap.campaign.customerselection

import java.sql.Timestamp

import com.jabong.dap.common.constants.variables.CustomerProductShortlistVariables
import com.jabong.dap.common.schema.SchemaUtils
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.Udf
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
  override def customerSelection(customerData: DataFrame, orderItemData: DataFrame): DataFrame = ???

  /**
   * This method will return DataFrame with column(fk_customer, sku, domain, user_device_type, created_at) from last n days
   * @param dfCustomerProductShortlist
   * @return DataFrame
   */
  override def customerSelection(dfCustomerProductShortlist: DataFrame): DataFrame = {

    if (dfCustomerProductShortlist == null) {

      logger.error("Data frame should not be null")

      return null

    }

    if (!SchemaUtils.isSchemaEqual(dfCustomerProductShortlist.schema, Schema.customerProductShortlist)) {

      logger.error("schema attributes or data type mismatch:" + dfCustomerProductShortlist.schema)

      logger.error("schema should be this :" + Schema.customerProductShortlist)
      return null

    }

    //FIXME: We are filtering cases where fk_customer is null and we have to check cases where fk_customer is null and email is not null
    // FIXME - - Order shouldn’t have been placed for the Ref SKU yet
    val dfResult = dfCustomerProductShortlist.filter(CustomerProductShortlistVariables.FK_CUSTOMER + " is not null and " +
      //col(CustomerProductShortlist.EMAIL) + " is not null ) and " +
      CustomerProductShortlistVariables.REMOVED_AT + " is null")
      .select(
        col(CustomerProductShortlistVariables.FK_CUSTOMER),
        col(CustomerProductShortlistVariables.EMAIL),
        col(CustomerProductShortlistVariables.SKU),
        col(CustomerProductShortlistVariables.DOMAIN),
        col(CustomerProductShortlistVariables.USER_DEVICE_TYPE),
        Udf.yyyymmdd(dfCustomerProductShortlist(CustomerProductShortlistVariables.CREATED_AT)) as CustomerProductShortlistVariables.CREATED_AT,
        Udf.simpleSkuFromExtraData(dfCustomerProductShortlist(CustomerProductShortlistVariables.EXTRA_DATA)) as CustomerProductShortlistVariables.SKU_SIMPLE,
        Udf.priceFromExtraData(dfCustomerProductShortlist(CustomerProductShortlistVariables.EXTRA_DATA)) as CustomerProductShortlistVariables.PRICE
      )
    dfResult
  }

  override def customerSelection(inData: DataFrame, inData2: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, inData3: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, ndays: Int): DataFrame = ???
}
