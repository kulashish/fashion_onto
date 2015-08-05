package com.jabong.dap.campaign.skuselection

import java.sql.Timestamp

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.variables.{ CustomerVariables, ProductVariables, ItrVariables, CustomerProductShortlistVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.{ UdfUtils, Udf }
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
      Udf.yyyymmddString(df30DaysItrData(ItrVariables.CREATED_AT)) as ItrVariables.ITR_ + ItrVariables.CREATED_AT
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
        //col(CustomerVariables.EMAIL),
        col(ItrVariables.SKU_SIMPLE) as ProductVariables.SKU_SIMPLE,
        col(ItrVariables.SPECIAL_PRICE) as ProductVariables.SPECIAL_PRICE)

    // FIXME: generate ref skus
    //  val refSkus = CampaignUtils.generateReferenceSkusForAcart(dfResult, CampaignCommon.NUMBER_REF_SKUS)

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
      //CustomerProductShortlistVariables.EMAIL,
      CustomerProductShortlistVariables.SKU_SIMPLE,
      CustomerProductShortlistVariables.SPECIAL_PRICE
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
      .withColumnRenamed(CustomerProductShortlistVariables.SKU, CustomerProductShortlistVariables.SKU_SIMPLE)
      .withColumnRenamed(CustomerProductShortlistVariables.AVERAGE_PRICE, CustomerProductShortlistVariables.SPECIAL_PRICE)

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
    val dfResult = dfUnion.select(col(CustomerProductShortlistVariables.FK_CUSTOMER),
      col(CustomerProductShortlistVariables.EMAIL),
      col(CustomerProductShortlistVariables.SKU_SIMPLE) as ProductVariables.SKU_SIMPLE,
      col(CustomerProductShortlistVariables.SPECIAL_PRICE) as ProductVariables.SPECIAL_PRICE
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

    val skuCustomerProductShortlist = dfCustomerProductShortlist.filter(CustomerProductShortlistVariables.SKU_SIMPLE + " is null or " + CustomerProductShortlistVariables.PRICE + " is null ")
      .select(
        CustomerProductShortlistVariables.FK_CUSTOMER,
        CustomerProductShortlistVariables.EMAIL,
        CustomerProductShortlistVariables.SKU,
        CustomerProductShortlistVariables.CREATED_AT
      )

    val irt30Day = df30DaysItrData.withColumnRenamed(ItrVariables.ITR_ + ItrVariables.AVERAGE_PRICE, CustomerProductShortlistVariables.AVERAGE_PRICE)

    val joinDf = skuCustomerProductShortlist.join(irt30Day, skuCustomerProductShortlist(CustomerProductShortlistVariables.SKU) === irt30Day(ItrVariables.ITR_ + ItrVariables.SKU)
      &&
      skuCustomerProductShortlist(CustomerProductShortlistVariables.CREATED_AT) === irt30Day(ItrVariables.ITR_ + ItrVariables.CREATED_AT), "inner")
      .select(
        CustomerProductShortlistVariables.FK_CUSTOMER,
        CustomerProductShortlistVariables.EMAIL,
        CustomerProductShortlistVariables.SKU,
        CustomerProductShortlistVariables.AVERAGE_PRICE
      )

    //join yesterdayItrData and joinDf on the basis of SKU
    //filter on the basis of AVERAGE_PRICE
    val dfResult = joinDf.join(dfYesterdayItrData, joinDf(CustomerProductShortlistVariables.SKU) === dfYesterdayItrData(ItrVariables.ITR_ + ItrVariables.SKU))
      .filter(CustomerProductShortlistVariables.AVERAGE_PRICE + " > " + ItrVariables.ITR_ + ItrVariables.AVERAGE_PRICE)
      .select(
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

    val skuSimpleCustomerProductShortlist = dfCustomerProductShortlist.filter(CustomerProductShortlistVariables.SKU_SIMPLE + " is not null and " + CustomerProductShortlistVariables.PRICE + " is not null ")
      .select(
        CustomerProductShortlistVariables.FK_CUSTOMER,
        CustomerProductShortlistVariables.EMAIL,
        CustomerProductShortlistVariables.SKU_SIMPLE,
        CustomerProductShortlistVariables.PRICE
      )

    val yesterdayItrData = dfYesterdayItrData.select(
      ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE,
      ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE
    )

    val dfJoin = skuSimpleCustomerProductShortlist.join(
      yesterdayItrData,
      skuSimpleCustomerProductShortlist(CustomerProductShortlistVariables.SKU_SIMPLE) === yesterdayItrData(ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE),
      "inner"
    )

    val dfFilter = dfJoin.filter(CustomerProductShortlistVariables.PRICE + " > " + ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE)

    val dfResult = dfFilter.select(
      col(CustomerProductShortlistVariables.FK_CUSTOMER),
      col(CustomerProductShortlistVariables.EMAIL),
      col(CustomerProductShortlistVariables.SKU_SIMPLE),
      col(CustomerProductShortlistVariables.PRICE) as CustomerProductShortlistVariables.SPECIAL_PRICE
    )

    return dfResult

  }

  // not needed
  override def skuFilter(inDataFrame: DataFrame): DataFrame = ???

  override def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame, campaignName: String): DataFrame = ???

  override def skuFilter(inDataFrame: DataFrame, inDataFrame2: DataFrame, inDataFrame3: DataFrame): DataFrame = ???

  override def skuFilter(dfCustomerPageVisit: DataFrame, dfItrData: DataFrame, dfCustomer: DataFrame, dfSalesOrder: DataFrame, dfSalesOrderItem: DataFrame): DataFrame = ???

  override def skuFilter(pastCampaignData: DataFrame, dfCustomerPageVisit: DataFrame, dfItrData: DataFrame, dfCustomer: DataFrame, dfSalesOrder: DataFrame, dfSalesOrderItem: DataFrame, campaignName: String): DataFrame = ???

}

