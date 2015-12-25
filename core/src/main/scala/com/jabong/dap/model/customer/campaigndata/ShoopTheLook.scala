package com.jabong.dap.model.customer.campaigndata

import java.math.BigDecimal

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.CampaignMergedFields
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.dataFeeds.DataFeedsModel
import grizzled.slf4j.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row }

import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
 * Created by raghu on 22/12/15.
 */
object ShoopTheLook extends DataFeedsModel with Logging {

  val REF_SKU = "refsku"
  val REC_SKU = "recsku"
  val FK_CATALOG_SHOP_LOOK = "fk_catalog_shop_look"
  val NUMBER_REF_SKUS = 5
  val IS_ACTIVE = "is_active"

  /**
   *
   * @param incrDate
   * @param saveMode
   * @return
   */
  override def canProcess(incrDate: String, saveMode: String): Boolean = {
    val pathShopTheLookIncr = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.EMAIL_CAMPAIGNS, DataSets.SHOP_THE_LOOK, DataSets.DAILY_MODE, incrDate)
    DataWriter.canWrite(saveMode, pathShopTheLookIncr)
  }

  /**
   *
   * @param incrDate
   * @param prevDate
   * @param paths
   * @return
   */
  override def readDF(incrDate: String, prevDate: String, paths: String): mutable.HashMap[String, DataFrame] = {

    val dfMap: HashMap[String, DataFrame] = new HashMap[String, DataFrame]()

    val dfCmr = CampaignInput.loadCustomerMasterData(incrDate)
    val dfSalesOrderIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, DataSets.DAILY_MODE, incrDate)
    val dfSalesOrderItemIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ITEM, DataSets.DAILY_MODE, incrDate)
    val dfCatalogShopLookDetailFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CATALOG_SHOP_LOOK_DETAIL, DataSets.FULL_MERGE_MODE, incrDate)
    val itrSkuSimpleYesterdayData = CampaignInput.loadYesterdayItrSimpleData(incrDate)

    dfMap.put("cmrFull", dfCmr)
    dfMap.put("dfSalesOrderIncr", dfSalesOrderIncr)
    dfMap.put("dfSalesOrderItemIncr", dfSalesOrderItemIncr)
    dfMap.put("dfCatalogShopLookDetailFull", dfCatalogShopLookDetailFull)
    dfMap.put("itrSkuSimpleYesterdayData", itrSkuSimpleYesterdayData)

    dfMap
  }

  /**
   *
   * @param dfMap
   * @return
   */
  override def process(dfMap: mutable.HashMap[String, DataFrame]): mutable.HashMap[String, DataFrame] = {

    val dfShopTheLookIncr = getShopTheLook(dfMap("dfSalesOrderIncr"), dfMap("dfSalesOrderItemIncr"), dfMap("dfCatalogShopLookDetailFull"), dfMap("itrSkuSimpleYesterdayData"))

    val dfWrite: HashMap[String, DataFrame] = new HashMap[String, DataFrame]()
    dfWrite.put("cmrFull", dfMap("cmrFull"))
    dfWrite.put("dfShopTheLookIncr", dfShopTheLookIncr)

    dfWrite
  }

  def getShopTheLook(dfSalesOrderIncr: DataFrame, dfSalesOrderItemIncr: DataFrame, dfCatalogShopLookDetailFull: DataFrame, yesterdayItrData: DataFrame): DataFrame = {

    val dfSO = dfSalesOrderIncr.select(
      SalesOrderVariables.ID_SALES_ORDER,
      SalesOrderVariables.FK_CUSTOMER
    ).distinct
    CampaignUtils.debug(dfSO, "dfSO")

    val dfSOI = dfSalesOrderItemIncr.select(
      col(SalesOrderItemVariables.FK_SALES_ORDER),
      Udf.skuFromSimpleSku(col(SalesOrderItemVariables.SKU)) as SalesOrderItemVariables.SKU,
      col(SalesOrderItemVariables.PAID_PRICE)
    ).distinct
    CampaignUtils.debug(dfSOI, "dfSOI")

    val CSLD = dfCatalogShopLookDetailFull.select(
      FK_CATALOG_SHOP_LOOK,
      SalesOrderItemVariables.SKU,
      IS_ACTIVE
    ).distinct.filter(IS_ACTIVE + " = 1")
      .select(
        FK_CATALOG_SHOP_LOOK,
        SalesOrderItemVariables.SKU
      )
    CampaignUtils.debug(CSLD, "CSLD")

    val dfItrData = yesterdayItrData.select(
      Udf.skuFromSimpleSku(col(ProductVariables.SKU_SIMPLE)) as ProductVariables.SKU_SIMPLE,
      col(ProductVariables.SPECIAL_PRICE)
    ).distinct
    CampaignUtils.debug(dfItrData, "dfItrData")

    val joinedSoSoi = dfSO.join(
      dfSOI,
      dfSO(SalesOrderVariables.ID_SALES_ORDER) === dfSOI(SalesOrderItemVariables.FK_SALES_ORDER),
      SQL.INNER
    ).select(
        dfSO(SalesOrderVariables.FK_CUSTOMER),
        dfSOI(SalesOrderItemVariables.SKU) as ProductVariables.SKU_SIMPLE,
        dfSOI(SalesOrderItemVariables.PAID_PRICE)
      )
    CampaignUtils.debug(joinedSoSoi, "joinedSoSoi")

    val skuInCSLD = joinedSoSoi.join(CSLD, joinedSoSoi(ProductVariables.SKU_SIMPLE) === CSLD(SalesOrderItemVariables.SKU), SQL.INNER)
      .select(
        SalesOrderVariables.FK_CUSTOMER,
        FK_CATALOG_SHOP_LOOK,
        SalesOrderItemVariables.SKU,
        SalesOrderItemVariables.PAID_PRICE
      )
    CampaignUtils.debug(skuInCSLD, "skuInCSLD")

    val skuNotInCSLD = CSLD.except(skuInCSLD.select(FK_CATALOG_SHOP_LOOK, SalesOrderItemVariables.SKU))
    CampaignUtils.debug(skuNotInCSLD, "skuNotInCSLD")

    val dfskuNotInCSLD = skuNotInCSLD.join(dfItrData, skuNotInCSLD(SalesOrderItemVariables.SKU) === dfItrData(ProductVariables.SKU_SIMPLE), SQL.INNER)
      .select(
        FK_CATALOG_SHOP_LOOK,
        ProductVariables.SKU,
        ProductVariables.SPECIAL_PRICE
      ).distinct
    CampaignUtils.debug(dfskuNotInCSLD, "dfskuNotInCSLD")

    val joindDf = skuInCSLD.join(dfskuNotInCSLD, skuInCSLD(FK_CATALOG_SHOP_LOOK) === dfskuNotInCSLD(FK_CATALOG_SHOP_LOOK), SQL.INNER)
      .select(
        skuInCSLD(SalesOrderVariables.FK_CUSTOMER).cast("string") as SalesOrderVariables.FK_CUSTOMER,
        skuInCSLD(SalesOrderItemVariables.PAID_PRICE),
        skuInCSLD(SalesOrderItemVariables.SKU) as REF_SKU,
        dfskuNotInCSLD(ProductVariables.SPECIAL_PRICE),
        dfskuNotInCSLD(ProductVariables.SKU) as REC_SKU
      )
    CampaignUtils.debug(joindDf, "joindDf")

    val skuMap = joindDf.map(t => (t(t.fieldIndex(SalesOrderVariables.FK_CUSTOMER)),
      (CampaignUtils.checkNullBigDecimalToDouble(t(t.fieldIndex(SalesOrderItemVariables.PAID_PRICE))),
        t(t.fieldIndex(REF_SKU)).asInstanceOf[String],
        t(t.fieldIndex(ProductVariables.SPECIAL_PRICE)).asInstanceOf[BigDecimal].doubleValue(),
        t(t.fieldIndex(REC_SKU)).asInstanceOf[String])))

    val dfGroup = skuMap.groupByKey().map { case (key, data) => (key, genListSkus(data.toList)) }
      .map(x => (x._1, splitRefRecSkus(x._2)))
      .map(x => (x._1.asInstanceOf[String], x._2._1, x._2._2, x._2._3, x._2._4, x._2._5, x._2._6, x._2._7, x._2._8, x._2._9, x._2._10))

    val sqlContext = Spark.getSqlContext()
    import sqlContext.implicits._
    val dfShopTheLookIncr = dfGroup.toDF(
      CustomerVariables.CUSTOMER_ID,
      REF_SKU + "1",
      REF_SKU + "2",
      REF_SKU + "3",
      REF_SKU + "4",
      REF_SKU + "5",
      REC_SKU + "1",
      REC_SKU + "2",
      REC_SKU + "3",
      REC_SKU + "4",
      REC_SKU + "5")

    CampaignUtils.debug(dfShopTheLookIncr, "dfShopTheLookIncr")
    dfShopTheLookIncr
  }

  /**
   *
   * @param sKusList
   * @return
   */
  def genListSkus(sKusList: scala.collection.immutable.List[(Double, String, Double, String)]): List[(Double, String, Double, String)] = {
    require(sKusList != null, "sKusList cannot be null")
    require(sKusList.size != 0, "sKusList cannot be empty")
    return sKusList
  }

  /**
   *
   * @param skusList
   * @return
   */
  def splitRefRecSkus(skusList: List[(Double, String, Double, String)]): Tuple10[String, String, String, String, String, String, String, String, String, String] = {
    var refSkusList = List[(Double, String)]()
    var recSkusList = List[(Double, String)]()

    for (a <- 0 until skusList.size) {
      refSkusList = refSkusList :+ (skusList(a)._1, skusList(a)._2)
      recSkusList = recSkusList :+ (skusList(a)._3, skusList(a)._4)
    }

    val list = addSku(refSkusList) ::: addSku(recSkusList)

    return Tuple10(list(0), list(1), list(2), list(3), list(4), list(5), list(6), list(7), list(8), list(9))
  }

  /**
   *
   * @param listSkus
   * @return
   */
  def addSku(listSkus: List[(Double, String)]): List[(String)] = {
    var result = List[(String)]()
    val list = listSkus.sortBy(-_._1).distinct

    for (a <- 0 until list.size if a < NUMBER_REF_SKUS) {
      result = result :+ list(a)._2.asInstanceOf[String]
    }

    result = result.distinct
    for (a <- result.size until NUMBER_REF_SKUS) {
      result = result :+ null.asInstanceOf[String]
    }
    result
  }

  /**
   *
   * @param dfWriteMap
   * @param saveMode
   * @param incrDate
   */
  override def write(dfWriteMap: mutable.HashMap[String, DataFrame], saveMode: String, incrDate: String): Unit = {

    val dfShopTheLookIncr = dfWriteMap("dfShopTheLookIncr")
    val pathShopTheLookIncr = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.EMAIL_CAMPAIGNS, DataSets.SHOP_THE_LOOK, DataSets.DAILY_MODE, incrDate)
    if (DataWriter.canWrite(saveMode, pathShopTheLookIncr)) {
      DataWriter.writeParquet(dfShopTheLookIncr, pathShopTheLookIncr, saveMode)
    }

    val dfCmrFull = dfWriteMap("cmrFull")
    val dfCmr = dfCmrFull.select(
      dfCmrFull(ContactListMobileVars.UID),
      dfCmrFull(CustomerVariables.ID_CUSTOMER).cast("string") as (CustomerVariables.ID_CUSTOMER)
    )

    val dfCsv = dfShopTheLookIncr.join(dfCmr, dfShopTheLookIncr(CustomerVariables.CUSTOMER_ID) === dfCmr(CustomerVariables.ID_CUSTOMER), SQL.LEFT_OUTER)
      .select(
        col(ContactListMobileVars.UID) as CustomerVariables.UID,
        col(CustomerVariables.CUSTOMER_ID),
        col(REF_SKU + "1"),
        col(REF_SKU + "2"),
        col(REF_SKU + "3"),
        col(REF_SKU + "4"),
        col(REF_SKU + "5"),
        col(REC_SKU + "1"),
        col(REC_SKU + "2"),
        col(REC_SKU + "3"),
        col(REC_SKU + "4"),
        col(REC_SKU + "5")
      )
      .filter(CustomerVariables.UID + " is not null")
      .na.fill("")

    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    DataWriter.writeCsv(dfCsv, DataSets.EMAIL_CAMPAIGNS, DataSets.SHOP_THE_LOOK, DataSets.DAILY_MODE, incrDate, fileDate + "_ShoptheLook_data", DataSets.IGNORE_SAVEMODE, "true", ";", 1)
  }
}
