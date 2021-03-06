package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.constants.variables.CustomerVariables
import com.jabong.dap.common.udf.Udf
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

    //    if (!SchemaUtils.isSchemaEqual(dfCustomerProductShortlist.schema, Schema.customerProductShortlist)) {
    //
    //      logger.error("schema attributes or data type mismatch:" + dfCustomerProductShortlist.schema)
    //
    //      logger.error("schema should be this :" + Schema.customerProductShortlist)
    //      return null
    //
    //    }

    // FIXME - - Order shouldn’t have been placed for the Ref SKU yet
    val dfResult = dfCustomerProductShortlist.filter("(" + CustomerVariables.FK_CUSTOMER + " is not null or " +
      CustomerVariables.EMAIL + " is not null ) and " +
      CustomerVariables.REMOVED_AT + " is null")
      .select(
        col(CustomerVariables.FK_CUSTOMER),
        col(CustomerVariables.EMAIL),
        col(CustomerVariables.SKU),
        col(CustomerVariables.DOMAIN),
        col(CustomerVariables.USER_DEVICE_TYPE),
        Udf.yyyymmdd(dfCustomerProductShortlist(CustomerVariables.CREATED_AT)) as CustomerVariables.CREATED_AT,
        Udf.simpleSkuFromExtraData(dfCustomerProductShortlist(CustomerVariables.EXTRA_DATA)) as CustomerVariables.SKU_SIMPLE,
        Udf.priceFromExtraData(dfCustomerProductShortlist(CustomerVariables.EXTRA_DATA)) as CustomerVariables.PRICE
      )
    dfResult
  }

  override def customerSelection(inData: DataFrame, inData2: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, inData3: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, ndays: Int): DataFrame = ???
}
