package com.jabong.dap.campaign.skuselection

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.variables.{ CustomerProductShortlistVariables, CustomerVariables, ItrVariables, ProductVariables }
import com.jabong.dap.common.udf.Udf
import grizzled.slf4j.Logging
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
  // dfCustomerProductShortlist =  [(id_customer, sku simple)]
  // itr30dayData = [(skusimple, date, special price)]
  override def skuFilter(customerSelected: DataFrame, df30DaysItrData: DataFrame): DataFrame = {

    if (customerSelected == null || df30DaysItrData == null) {

      logger.error("Data frame should not be null")

      return null

    }

    val itr30dayData = df30DaysItrData.select(
      col(ItrVariables.SKU_SIMPLE) as ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE,
      col(ItrVariables.SPECIAL_PRICE) as ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE,
      Udf.yyyymmdd(df30DaysItrData(ItrVariables.CREATED_AT)) as ItrVariables.ITR_ + ItrVariables.CREATED_AT
    )

    //filter yesterday itrData from itr30dayData
    val dfYesterdayItrData = CampaignUtils.getYesterdayItrData(itr30dayData)

    // for previous price, rename it to ItrVariables.SPECIAL_PRICE
    val irt30Day = itr30dayData.withColumnRenamed(ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE, ItrVariables.SPECIAL_PRICE)

    val join30DaysDf = getJoinDF(customerSelected, irt30Day)

    //join yesterdayItrData and joinDf on the basis of SKU
    //filter on the basis of SPECIAL_PRICE
    val dfResult = join30DaysDf.join(dfYesterdayItrData, join30DaysDf(ItrVariables.SKU_SIMPLE) === dfYesterdayItrData(ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE))
      .filter(ItrVariables.SPECIAL_PRICE + " > " + ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE)
      .select(
        col(CustomerVariables.FK_CUSTOMER),
        col(CustomerVariables.EMAIL),
        col(ItrVariables.SKU_SIMPLE) as ProductVariables.SKU_SIMPLE,
        col(ItrVariables.SPECIAL_PRICE) as ProductVariables.SPECIAL_PRICE)

    // FIXME: generate ref skus

    return dfResult
  }

  /**
   * join CustomerProductShortlistVariables and itr30dayData on the basis of SKU and CREATED_AT
   * @note From this we can get SPECIAL_PRICE when customer added it into CustomerProductShortlist
   * @param cpsl
   * @param itr30dayData
   */
  def getJoinDF(cpsl: DataFrame, itr30dayData: DataFrame): DataFrame = {

    val joinDf = cpsl.join(itr30dayData, cpsl(CustomerProductShortlistVariables.SKU_SIMPLE) === itr30dayData(ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE)
      &&
      cpsl(CustomerProductShortlistVariables.CREATED_AT) === itr30dayData(ItrVariables.ITR_ + ItrVariables.CREATED_AT), "inner")

    val dfResult = joinDf.select(
      CustomerProductShortlistVariables.FK_CUSTOMER,
      CustomerProductShortlistVariables.EMAIL,
      CustomerProductShortlistVariables.SKU_SIMPLE,
      CustomerProductShortlistVariables.SPECIAL_PRICE
    )

    return dfResult

  }

  // not needed
  override def skuFilter(inDataFrame: DataFrame): DataFrame = ???

  override def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame, campaignName: String): DataFrame = ???

  override def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame, inDataFrame3: DataFrame): DataFrame = ???
}

