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
    CampaignUtils.debug(CSLD, "CSLD")

    val dfItrData = yesterdayItrData.select(
      Udf.skuFromSimpleSku(col(ProductVariables.SKU_SIMPLE)) as ProductVariables.SKU_SIMPLE,
      col(ProductVariables.SPECIAL_PRICE)
    ).distinct
    CampaignUtils.debug(dfItrData, "dfItrData")

    val dfCSLD = CSLD.join(dfItrData, CSLD(SalesOrderItemVariables.SKU) === dfItrData(ProductVariables.SKU_SIMPLE), SQL.INNER)
      .select(
        FK_CATALOG_SHOP_LOOK,
        SalesOrderItemVariables.SKU,
        ProductVariables.SPECIAL_PRICE
      ).distinct
    CampaignUtils.debug(dfCSLD, "dfCSLD")

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

    val skuInCSLD = joinedSoSoi.join(dfCSLD, joinedSoSoi(ProductVariables.SKU_SIMPLE) === dfCSLD(SalesOrderItemVariables.SKU), SQL.INNER)
    CampaignUtils.debug(skuInCSLD, "skuInCSLD")
    //    val skuNotInCSLD = joinedSoSoi.join(dfCSLD, joinedSoSoi(ProductVariables.SKU_SIMPLE).notEqual(dfCSLD(SalesOrderItemVariables.SKU)), SQL.INNER)
    //    CampaignUtils.debug(skuNotInCSLD, "skuNotInCSLD")
    val dfSkuNotInCSLD = skuInCSLD.join(dfCSLD, skuInCSLD(FK_CATALOG_SHOP_LOOK) === dfCSLD(FK_CATALOG_SHOP_LOOK), SQL.INNER)
      .select(
        skuInCSLD(FK_CATALOG_SHOP_LOOK),
        skuInCSLD(ProductVariables.SKU_SIMPLE) as REF_SKU,
        dfCSLD(ProductVariables.SKU) as REC_SKU,
        dfCSLD(ProductVariables.SPECIAL_PRICE)
      )
    CampaignUtils.debug(dfSkuNotInCSLD, "dfSkuNotInCSLD")

    val skuNotInCSLD = dfSkuNotInCSLD.filter(REF_SKU + " is null")
      .select(
        dfSkuNotInCSLD(FK_CATALOG_SHOP_LOOK),
        dfSkuNotInCSLD(REC_SKU),
        dfSkuNotInCSLD(ProductVariables.SPECIAL_PRICE)
      )
    CampaignUtils.debug(skuNotInCSLD, "skuNotInCSLD")

    val joindDf = skuInCSLD.join(skuNotInCSLD, skuInCSLD(FK_CATALOG_SHOP_LOOK) === skuNotInCSLD(FK_CATALOG_SHOP_LOOK), SQL.LEFT_OUTER)
      .select(
        skuInCSLD(SalesOrderVariables.FK_CUSTOMER).cast("string") as SalesOrderVariables.FK_CUSTOMER,
        skuInCSLD(ProductVariables.SKU_SIMPLE) as REF_SKU,
        skuInCSLD(SalesOrderItemVariables.PAID_PRICE),
        skuNotInCSLD(REC_SKU),
        skuNotInCSLD(ProductVariables.SPECIAL_PRICE)
      ).filter(CustomerVariables.FK_CUSTOMER + " != 0  and " + CustomerVariables.FK_CUSTOMER + " is not null")
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
    val grouped = dfGroup.toDF(
      CustomerVariables.CUSTOMER_ID,
      REF_SKU + "1",
      REC_SKU + "1",
      REF_SKU + "2",
      REC_SKU + "2",
      REF_SKU + "3",
      REC_SKU + "3",
      REF_SKU + "4",
      REC_SKU + "4",
      REF_SKU + "5",
      REC_SKU + "5")

    CampaignUtils.debug(grouped, "grouped")
    grouped
  }

  /**
   *
   * @param refSKusList
   * @return
   */
  def genListSkus(refSKusList: scala.collection.immutable.List[(Double, String, Double, String)]): List[(Double, String, Double, String)] = {
    require(refSKusList != null, "refSkusList cannot be null")
    require(refSKusList.size != 0, "refSkusList cannot be empty")
    val refList = refSKusList.sortBy(-_._1).distinct
    val recList = refSKusList.sortBy(-_._3).distinct
    val listSize = refList.size
    var list = List[(Double, String, Double, String)]()
    for (a <- 0 until listSize) {
      list ::= (refList(a)._1, refList(a)._2, recList(a)._3, recList(a)._4)
    }
    return list
  }

  /**
   *
   * @param refRecSKusList
   * @return
   */
  def splitRefRecSkus(refRecSKusList: List[(Double, String, Double, String)]): Tuple10[String, String, String, String, String, String, String, String, String, String] = {
    val listSize = refRecSKusList.size
    var list = List[(String)]()
    for (a <- 0 until listSize if a < NUMBER_REF_SKUS) {
      list = List(refRecSKusList(a)._2.asInstanceOf[String], refRecSKusList(a)._4.asInstanceOf[String]).:::(list)
    }
    for (a <- listSize until NUMBER_REF_SKUS) {
      list = List(null.asInstanceOf[String], null.asInstanceOf[String]).:::(list)
    }

    return Tuple10(list(0), list(1), list(2), list(3), list(4), list(5), list(6), list(7), list(8), list(9))
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
