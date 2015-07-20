package com.jabong.dap.campaign.skuselection

import java.sql.Timestamp

import com.jabong.dap.common.constants.variables.{ CustomerVariables, ProductVariables, ItrVariables, CustomerProductShortlistVariables }
import com.jabong.dap.common.schema.SchemaUtils
import com.jabong.dap.common.time.{ Constants, TimeUtils }
import com.jabong.dap.common.udf.{ UdfUtils, Udf }
import com.jabong.dap.data.storage.schema.Schema
import grizzled.slf4j.{ Logging }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Item On Discount Execution Class
 */
class ItemOnDiscount extends SkuSelector with Logging {

  // sku filter
  // 2. Today's Special Price of SKU (SIMPLE – include size) is less than
  //      previous Special Price of SKU (when it was added to wishlist)
  // 3. This campaign shouldn’t have gone to the customer in the past 30 days for the same Ref SKU
  // 4. pick based on special price (descending)
  //
  // dfCustomerProductShortlist =  [(id_customer, sku, sku simple)]
  // itr30dayData = [(skusimple, date, special price)]
  override def skuFilter(customerSelected: DataFrame, df30DaysItrData: DataFrame, campaignName: String): DataFrame = {

    if (customerSelected == null || df30DaysItrData == null) {

      logger.error("Data frame should not be null")

      return null

    }

    // FIXME: either sku or sku simple: both can't be there
    val itr30dayData = df30DaysItrData.select(
      col(ItrVariables.SKU) as ItrVariables.ITR_ + ItrVariables.SKU,
      col(ItrVariables.AVERAGE_PRICE) as ItrVariables.ITR_ + ItrVariables.AVERAGE_PRICE,
      col(ItrVariables.SIMPLE_SKU) as ItrVariables.ITR_ + ItrVariables.SIMPLE_SKU,
      col(ItrVariables.SPECIAL_PRICE) as ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE,
      Udf.yyyymmdd(df30DaysItrData(ItrVariables.CREATED_AT)) as ItrVariables.ITR_ + ItrVariables.CREATED_AT
    )

    //get data yesterday date
    val yesterdayDate = Timestamp.valueOf(TimeUtils.getDateAfterNDays(-1, Constants.DATE_TIME_FORMAT))

    val yesterdayDateYYYYmmDD = UdfUtils.getYYYYmmDD(yesterdayDate)

    //filter yesterday itrData from itr30dayData
    val dfYesterdayItrData = itr30dayData.filter(ItrVariables.ITR_ + ItrVariables.CREATED_AT + " = " + "'" + yesterdayDateYYYYmmDD + "'")
    
    // for previous price, rename it to ItrVariables.SPECIAL_PRICE
    val irt30Day = df30DaysItrData.withColumnRenamed(ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE, ItrVariables.SPECIAL_PRICE)

    val join30DaysDf = getJoinDF(customerSelected, irt30Day)

    //join yesterdayItrData and joinDf on the basis of SKU
    //filter on the basis of SPECIAL_PRICE
    val dfResult = join30DaysDf.join(dfYesterdayItrData, join30DaysDf(ProductVariables.SKU_SIMPLE) === dfYesterdayItrData(ItrVariables.ITR_ + ItrVariables.SIMPLE_SKU))
      .filter(join30DaysDf(ItrVariables.SPECIAL_PRICE) + " > " + ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE)
      .select(
        col(CustomerVariables.FK_CUSTOMER),
        col(CustomerVariables.EMAIL),
        col(ProductVariables.SKU_SIMPLE))
    
    // FIXME: include special price
    
    // FIXME: generate ref skus
    

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

    val skuCustomerProductShortlist = dfCustomerProductShortlist.filter(CustomerProductShortlistVariables.SIMPLE_SKU + " is null ")
      .select(
        CustomerProductShortlistVariables.FK_CUSTOMER,
        CustomerProductShortlistVariables.EMAIL,
        CustomerProductShortlistVariables.SKU,
        CustomerProductShortlistVariables.CREATED_AT
      )

    val irt30Day = df30DaysItrData.withColumnRenamed(ItrVariables.ITR_ + ItrVariables.AVERAGE_PRICE, CustomerProductShortlistVariables.AVERAGE_PRICE)

    val joinDf = getJoinDF(skuCustomerProductShortlist, irt30Day)

    //join yesterdayItrData and joinDf on the basis of SKU
    //filter on the basis of AVERAGE_PRICE
    val dfResult = joinDf.join(dfYesterdayItrData, joinDf(CustomerProductShortlistVariables.SKU) === dfYesterdayItrData(ItrVariables.ITR_ + ItrVariables.SKU))
      .filter(CustomerProductShortlistVariables.AVERAGE_PRICE + " > " + ItrVariables.ITR_ + ItrVariables.AVERAGE_PRICE)
      .select(
        col(CustomerProductShortlistVariables.FK_CUSTOMER),
        col(CustomerProductShortlistVariables.EMAIL),
        col(CustomerProductShortlistVariables.SKU))

    return dfResult

  }

  /**
   * join CustomerProductShortlistVariables and itr30dayData on the basis of SKU and CREATED_AT
   * @note From this we can get AVERAGE_PRICE when customer added it into CustomerProductShortlist
   * @param cpsl
   * @param itr30dayData
   */
  def getJoinDF(cpsl: DataFrame, itr30dayData: DataFrame): DataFrame = {

    // FIXME: sku to sku simple
    val joinDf = cpsl.join(itr30dayData, cpsl(CustomerProductShortlistVariables.SKU) === itr30dayData(ItrVariables.ITR_ + ItrVariables.SKU)
      &&
      cpsl(CustomerProductShortlistVariables.CREATED_AT) === itr30dayData(ItrVariables.ITR_ + ItrVariables.CREATED_AT), "inner")

    
    // FIXME: special price
    val dfResult = joinDf.select(
      CustomerProductShortlistVariables.FK_CUSTOMER,
      CustomerProductShortlistVariables.EMAIL,
      CustomerProductShortlistVariables.SKU,
      CustomerProductShortlistVariables.AVERAGE_PRICE
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

    val skuSimpleCustomerProductShortlist = dfCustomerProductShortlist.filter(CustomerProductShortlistVariables.SIMPLE_SKU + " is not null ")
      .select(
        CustomerProductShortlistVariables.FK_CUSTOMER,
        CustomerProductShortlistVariables.EMAIL,
        CustomerProductShortlistVariables.SIMPLE_SKU,
        CustomerProductShortlistVariables.PRICE
      )

    val yesterdayItrData = dfYesterdayItrData.select(
      ItrVariables.ITR_ + ItrVariables.SIMPLE_SKU,
      ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE
    )

    val dfJoin = skuSimpleCustomerProductShortlist.join(
      yesterdayItrData,
      skuSimpleCustomerProductShortlist(CustomerProductShortlistVariables.SIMPLE_SKU) === yesterdayItrData(ItrVariables.ITR_ + ItrVariables.SIMPLE_SKU),
      "inner"
    )

    val dfFilter = dfJoin.filter(CustomerProductShortlistVariables.PRICE + " > " + ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE)

    val dfResult = dfFilter.select(
      col(CustomerProductShortlistVariables.FK_CUSTOMER),
      col(CustomerProductShortlistVariables.EMAIL),
      Udf.skuFromSimpleSku(dfJoin(CustomerProductShortlistVariables.SIMPLE_SKU)) as CustomerProductShortlistVariables.SKU
    )

    return dfResult

  }

  // not needed
  override def skuFilter(inDataFrame: DataFrame): DataFrame = ???

  override def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame): DataFrame = ???
}

