package com.jabong.dap.campaign.skuselection

import java.sql.Timestamp

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.{ Udf, UdfUtils }
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Item On Discount Execution Class
 */
object ItemOnDiscount extends Logging {

  // sku filter
  // 2. Today's Special Price of SKU (SIMPLE – include size) is less than
  //      previous Special Price of SKU (when it was added to wishlist)
  // 3. This campaign shouldn’t have gone to the customer in the past 30 days for the same Ref SKU
  // 4. pick based on special price (descending)
  //
  // dfCustomerProductShortlist =  [(id_customer, sku simple)]
  // itr30dayData = [(skusimple, date, special price)]
  def skuFilter(customerSelected: DataFrame, df30DaysItrData: DataFrame): DataFrame = {

    if (customerSelected == null || df30DaysItrData == null) {

      logger.error("Data frame should not be null")

      return null

    }

    val itr30dayData = df30DaysItrData.select(
      col(ItrVariables.SKU_SIMPLE) as ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE,
      col(ItrVariables.SPECIAL_PRICE) as ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE,
      Udf.yyyymmddString(df30DaysItrData(ItrVariables.CREATED_AT)) as ItrVariables.ITR_ + ItrVariables.CREATED_AT,
      col(ItrVariables.BRAND) as ItrVariables.ITR_ + ItrVariables.BRAND,
      col(ItrVariables.BRICK) as ItrVariables.ITR_ + ItrVariables.BRICK,
      col(ItrVariables.MVP) as ItrVariables.ITR_ + ItrVariables.MVP,
      col(ItrVariables.GENDER) as ItrVariables.ITR_ + ItrVariables.GENDER,
      col(ProductVariables.PRODUCT_NAME) as ItrVariables.ITR_ + ProductVariables.PRODUCT_NAME
    )

    var dfCustomerSelected: DataFrame = customerSelected

    //for InvalidIODCampaign: In SalesOrder Variable customer_email rename as email
    if (customerSelected.schema.fieldNames.toList.contains(SalesOrderVariables.CUSTOMER_EMAIL)) {
      dfCustomerSelected = customerSelected.withColumnRenamed(SalesOrderVariables.CUSTOMER_EMAIL, CustomerVariables.EMAIL)
    } else if (!customerSelected.schema.fieldNames.toList.contains(CustomerVariables.EMAIL)) {
      dfCustomerSelected = customerSelected.withColumn(CustomerVariables.EMAIL, lit(null))
    }

    //filter yesterday itrData from itr30dayData
    val dfYesterdayItrData = CampaignUtils.getYesterdayItrData(itr30dayData)
    val updatedCustomerSelected = dfCustomerSelected.select(
      Udf.yyyymmdd(customerSelected(CustomerVariables.CREATED_AT)) as CustomerVariables.CREATED_AT,
      col(CustomerVariables.FK_CUSTOMER),
      col(CustomerVariables.EMAIL),
      col (ProductVariables.SKU_SIMPLE)
    )

    // for previous price, rename it to ItrVariables.SPECIAL_PRICE
    val irt30Day = itr30dayData.withColumnRenamed(ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE, ItrVariables.SPECIAL_PRICE)

    val join30DaysDf = getJoinDF(updatedCustomerSelected, irt30Day)

    logger.info("joined customer selected with 30 days itr data")

    //join yesterdayItrData and joinDf on the basis of SKU
    //filter on the basis of SPECIAL_PRICE
    val dfResult = join30DaysDf.join(dfYesterdayItrData, join30DaysDf(ItrVariables.SKU_SIMPLE) === dfYesterdayItrData(ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE))
      .filter(ItrVariables.SPECIAL_PRICE + " > " + ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE)
      .select(
        col(CustomerVariables.FK_CUSTOMER),
        col(CustomerVariables.EMAIL),
        col(ItrVariables.SKU_SIMPLE) as ProductVariables.SKU_SIMPLE,
        col(ItrVariables.SPECIAL_PRICE) as ProductVariables.SPECIAL_PRICE,
        col(ItrVariables.ITR_ + ItrVariables.BRAND) as ProductVariables.BRAND,
        col(ItrVariables.ITR_ + ItrVariables.BRICK) as ProductVariables.BRICK,
        col(ItrVariables.ITR_ + ItrVariables.MVP) as ProductVariables.MVP,
        col(ItrVariables.ITR_ + ItrVariables.GENDER) as ProductVariables.GENDER,
        col(ItrVariables.ITR_ + ProductVariables.PRODUCT_NAME) as ProductVariables.PRODUCT_NAME)

    logger.info("After sku filter based on special price")

    //val refSkus = CampaignUtils.generateReferenceSkusForAcart(dfResult, CampaignCommon.NUMBER_REF_SKUS)

    //logger.info("After reference sku generation")

    return dfResult
  }

  /**
   *
   * @param itr30dayData
   * @return
   */
  def getYesterdayItrData(itr30dayData: DataFrame): DataFrame = {
    //get data yesterday date
    val yesterdayDate = Timestamp.valueOf(TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_TIME_FORMAT))

    val yesterdayDateYYYYmmDD = UdfUtils.getYYYYmmDD(yesterdayDate)

    //filter yesterday itrData from itr30dayData
    val dfYesterdayItrData = itr30dayData.filter(ItrVariables.ITR_ + ItrVariables.CREATED_AT + " = " + "'" + yesterdayDateYYYYmmDD + "'")

    return dfYesterdayItrData
  }

  /**
   * join CustomerVariables and itr30dayData on the basis of SKU and CREATED_AT
   * @note From this we can get SPECIAL_PRICE when customer added it into CustomerProductShortlist
   * @param cpsl
   * @param itr30dayData
   */
  def getJoinDF(cpsl: DataFrame, itr30dayData: DataFrame): DataFrame = {

    val joinDf = cpsl.join(itr30dayData, cpsl(CustomerVariables.SKU_SIMPLE) === itr30dayData(ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE)
      &&
      cpsl(CustomerVariables.CREATED_AT) === itr30dayData(ItrVariables.ITR_ + ItrVariables.CREATED_AT), SQL.INNER)

    val dfResult = joinDf.select(
      CustomerVariables.FK_CUSTOMER,
      CustomerVariables.EMAIL,
      CustomerVariables.SKU_SIMPLE,
      CustomerVariables.SPECIAL_PRICE
    )

    return dfResult

  }

  /**
   *
   * @param dfCustomerProductShortlist
   * @param df30DaysSkuItrData
   * @param dfYesterdaySkuSimpleItrData
   * @return
   */
  def shortListFullSkuFilter(dfCustomerProductShortlist: DataFrame, df30DaysSkuItrData: DataFrame, dfYesterdaySkuSimpleItrData: DataFrame): DataFrame = {

    //=====================================calculate SKU data frame=====================================================
    val itr30dayData = df30DaysSkuItrData.select(
      col(ItrVariables.SKU) as ItrVariables.ITR_ + ItrVariables.SKU,
      col(ItrVariables.AVERAGE_PRICE) as ItrVariables.ITR_ + ItrVariables.AVERAGE_PRICE,
      Udf.yyyymmdd(df30DaysSkuItrData(ItrVariables.CREATED_AT)) as ItrVariables.ITR_ + ItrVariables.CREATED_AT
    )

    //========df30DaysSkuItrData filter for yesterday===================================================================
    val dfYesterdayItrData = getYesterdayItrData(itr30dayData)

    val dfSkuLevel = shortListSkuFilter(dfCustomerProductShortlist, dfYesterdayItrData, itr30dayData)
      .withColumnRenamed(CustomerVariables.SKU, CustomerVariables.SKU_SIMPLE)
      .withColumnRenamed(CustomerVariables.AVERAGE_PRICE, CustomerVariables.SPECIAL_PRICE)

    //=========calculate SKU_SIMPLE data frame==========================================================================
    val yesterdaySkuSimpleItrData = dfYesterdaySkuSimpleItrData.select(
      col(ItrVariables.SKU_SIMPLE) as ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE,
      col(ItrVariables.SPECIAL_PRICE) as ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE
    )
    val dfSkuSimpleLevel = shortListSkuSimpleFilter(dfCustomerProductShortlist, yesterdaySkuSimpleItrData)

    //=======union both sku and sku simple==============================================================================
    val dfUnion = dfSkuLevel.unionAll(dfSkuSimpleLevel)

    //=========SKU_SIMPLE is mix of sku and sku-simple in case of shortlist======================================
    //=======select FK_CUSTOMER, EMAIL, SKU_SIMPLE, SPECIAL_PRICE=======================================================
    val dfResult = dfUnion.select(col(CustomerVariables.FK_CUSTOMER),
      col(CustomerVariables.EMAIL),
      col(CustomerVariables.SKU_SIMPLE) as ProductVariables.SKU_SIMPLE,
      col(CustomerVariables.SPECIAL_PRICE) as ProductVariables.SPECIAL_PRICE
    )

    return dfResult
  }

  /**
   * shortListSkuFilter will calculate data from YesterdayItrData and dfCustomerProductShortlist on the basis of SKU
   * @param dfCustomerProductShortlist
   * @param dfYesterdayItrData
   * @param df30DaysItrData
   * @return DataFrame
   */

  def shortListSkuFilter(dfCustomerProductShortlist: DataFrame, dfYesterdayItrData: DataFrame, df30DaysItrData: DataFrame): DataFrame = {

    val skuCustomerProductShortlist = dfCustomerProductShortlist.filter(CustomerVariables.SKU_SIMPLE + " is null or " + CustomerVariables.PRICE + " is null ")
      .select(
        CustomerVariables.FK_CUSTOMER,
        CustomerVariables.EMAIL,
        CustomerVariables.SKU,
        CustomerVariables.CREATED_AT
      )

    val irt30Day = df30DaysItrData.withColumnRenamed(ItrVariables.ITR_ + ItrVariables.AVERAGE_PRICE, CustomerVariables.AVERAGE_PRICE)

    val joinDf = skuCustomerProductShortlist.join(irt30Day, skuCustomerProductShortlist(CustomerVariables.SKU) === irt30Day(ItrVariables.ITR_ + ItrVariables.SKU)
      &&
      skuCustomerProductShortlist(CustomerVariables.CREATED_AT) === irt30Day(ItrVariables.ITR_ + ItrVariables.CREATED_AT), SQL.INNER)
      .select(
        CustomerVariables.FK_CUSTOMER,
        CustomerVariables.EMAIL,
        CustomerVariables.SKU,
        CustomerVariables.AVERAGE_PRICE
      )

    //join yesterdayItrData and joinDf on the basis of SKU
    //filter on the basis of AVERAGE_PRICE
    val dfResult = joinDf.join(dfYesterdayItrData, joinDf(CustomerVariables.SKU) === dfYesterdayItrData(ItrVariables.ITR_ + ItrVariables.SKU))
      .filter(CustomerVariables.AVERAGE_PRICE + " > " + ItrVariables.ITR_ + ItrVariables.AVERAGE_PRICE)
      .select(
        CustomerVariables.FK_CUSTOMER,
        CustomerVariables.EMAIL,
        CustomerVariables.SKU,
        CustomerVariables.AVERAGE_PRICE
      )

    return dfResult

  }

  /**
   *  * shortListSkuSimpleFilter will calculate data from YesterdayItrData and dfCustomerProductShortlist on the basis of simple_sku
   * @param dfCustomerProductShortlist
   * @param dfYesterdayItrData
   * @return DataFrame
   */
  def shortListSkuSimpleFilter(dfCustomerProductShortlist: DataFrame, dfYesterdayItrData: DataFrame): DataFrame = {

    val skuSimpleCustomerProductShortlist = dfCustomerProductShortlist.filter(CustomerVariables.SKU_SIMPLE + " is not null and " + CustomerVariables.PRICE + " is not null ")
      .select(
        CustomerVariables.FK_CUSTOMER,
        CustomerVariables.EMAIL,
        CustomerVariables.SKU_SIMPLE,
        CustomerVariables.PRICE
      )

    val yesterdayItrData = dfYesterdayItrData.select(
      ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE,
      ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE
    )

    val dfJoin = skuSimpleCustomerProductShortlist.join(
      yesterdayItrData,
      skuSimpleCustomerProductShortlist(CustomerVariables.SKU_SIMPLE) === yesterdayItrData(ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE),
      SQL.INNER
    )

    val dfFilter = dfJoin.filter(CustomerVariables.PRICE + " > " + ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE)

    val dfResult = dfFilter.select(
      col(CustomerVariables.FK_CUSTOMER),
      col(CustomerVariables.EMAIL),
      col(CustomerVariables.SKU_SIMPLE),
      col(CustomerVariables.PRICE) as CustomerVariables.SPECIAL_PRICE
    )

    return dfResult

  }
}

