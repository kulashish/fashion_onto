package com.jabong.dap.campaign.customerselection

import java.sql.Timestamp

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.variables.{ ProductVariables, ItrVariables, CustomerProductShortlistVariables }
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

    val dateBeforeNdays = TimeUtils.getDateAfterNDays(-ndays, TimeConstants.DATE_TIME_FORMAT_MS)

    val startTimestamp = TimeUtils.getStartTimestampMS(Timestamp.valueOf(dateBeforeNdays))

    val yesterdayDate = TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_TIME_FORMAT_MS)

    val endTimeStamp = TimeUtils.getEndTimestampMS(Timestamp.valueOf(yesterdayDate))

    //FIXME: We are filtering cases where fk_customer is null and we have to check cases where fk_customer is null and email is not null
    // FIXME - - Order shouldn’t have been placed for the Ref SKU yet
    val dfResult = dfCustomerProductShortlist.filter(CustomerProductShortlistVariables.FK_CUSTOMER + " is not null and " +
      //col(CustomerProductShortlist.EMAIL) + " is not null ) and " +
      CustomerProductShortlistVariables.REMOVED_AT + " is null and " +
      "'" + startTimestamp + "'" + " <= " + CustomerProductShortlistVariables.CREATED_AT + " and " + CustomerProductShortlistVariables.CREATED_AT + " <= " + "'" + endTimeStamp + "'")
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

  override def customerSelection(dfCustomerProductShortlist: DataFrame, df30DaysSkuItrData: DataFrame, dfYesterdaySkuSimpleItrData: DataFrame): DataFrame = {
    //=====================================calculate SKU data frame=====================================================
    val itr30dayData = df30DaysSkuItrData.select(
      col(ItrVariables.SKU) as ItrVariables.ITR_ + ItrVariables.SKU,
      col(ItrVariables.AVERAGE_PRICE) as ItrVariables.ITR_ + ItrVariables.AVERAGE_PRICE,
      Udf.yyyymmdd(df30DaysSkuItrData(ItrVariables.CREATED_AT)) as ItrVariables.ITR_ + ItrVariables.CREATED_AT
    )

    //========df30DaysSkuItrData filter for yesterday===================================================================
    val dfYesterdayItrData = CampaignUtils.getYesterdayItrData(itr30dayData)

    val dfSkuLevel = CampaignUtils.shortListSkuItrJoin(dfCustomerProductShortlist, dfYesterdayItrData, itr30dayData)
      .select(
        CustomerProductShortlistVariables.FK_CUSTOMER,
        CustomerProductShortlistVariables.EMAIL,
        CustomerProductShortlistVariables.SKU,
        CustomerProductShortlistVariables.AVERAGE_PRICE
      )

      .withColumnRenamed(CustomerProductShortlistVariables.SKU, CustomerProductShortlistVariables.SKU_SIMPLE)
      .withColumnRenamed(CustomerProductShortlistVariables.AVERAGE_PRICE, CustomerProductShortlistVariables.SPECIAL_PRICE)

    //=========calculate SKU_SIMPLE data frame==========================================================================
    val yesterdaySkuSimpleItrData = dfYesterdaySkuSimpleItrData.select(
      col(ItrVariables.SKU_SIMPLE) as ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE,
      col(ItrVariables.SPECIAL_PRICE) as ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE
    )
    val dfSkuSimpleLevel = CampaignUtils.shortListSkuSimpleItrJoin(dfCustomerProductShortlist, yesterdaySkuSimpleItrData)
      .select(
        col(CustomerProductShortlistVariables.FK_CUSTOMER),
        col(CustomerProductShortlistVariables.EMAIL),
        col(CustomerProductShortlistVariables.SKU_SIMPLE),
        col(CustomerProductShortlistVariables.PRICE) as CustomerProductShortlistVariables.SPECIAL_PRICE
      )
    //=======union both sku and sku simple==============================================================================
    val dfUnion = dfSkuLevel.unionAll(dfSkuSimpleLevel)

    //=========SKU_SIMPLE is mix of sku and sku-simple in case of shortlist======================================
    //=======select FK_CUSTOMER, EMAIL, SKU_SIMPLE, SPECIAL_PRICE=======================================================
    val dfResult = dfUnion.select(col(CustomerProductShortlistVariables.FK_CUSTOMER),
      col(CustomerProductShortlistVariables.EMAIL),
      col(CustomerProductShortlistVariables.SKU_SIMPLE) as ProductVariables.SKU_SIMPLE,
      col(CustomerProductShortlistVariables.SPECIAL_PRICE) as ProductVariables.SPECIAL_PRICE
    )

    return dfResult
  }

  override def customerSelection(inData: DataFrame, inData2: DataFrame, ndays: Int): DataFrame = ???

}
