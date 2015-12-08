package com.jabong.dap.model.city

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ ProductVariables, SalesAddressVariables, SalesOrderItemVariables, SalesOrderVariables }
import com.jabong.dap.common.{ Spark, Utils }
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.OrderBySchema
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.dataFeeds.DataFeedsModel
import grizzled.slf4j.Logging
import org.apache.spark.sql.{ DataFrame, Row }

import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
 * Created by rahul on 16/11/15.
 */
object CityData extends DataFeedsModel with Logging {

  val sqlContext = Spark.getSqlContext()

  override def canProcess(incrDate: String, saveMode: String): Boolean = {
    val cityWiseDataPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CITY_WISE_DATA, DataSets.FULL_MERGE_MODE, incrDate)
    val res = DataWriter.canWrite(saveMode, cityWiseDataPath)
    logger.error(cityWiseDataPath + "already Exists")
    res
  }

  override def readDF(incrDate: String, prevDate: String, paths: String): mutable.HashMap[String, DataFrame] = {
    val dfMap = new HashMap[String, DataFrame]()
    var mode = DataSets.FULL_MERGE_MODE
    if (null == paths) {
      mode = DataSets.DAILY_MODE
      logger.info("Running in Daily Mode")
      val dfCityWisePrevFull = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CITY_WISE_DATA, DataSets.FULL_MERGE_MODE, prevDate)
      dfMap.put("cityWisePrevFullData", dfCityWisePrevFull)
    }
    val dfSalesOrderIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, mode, incrDate)
    dfMap.put("salesOrderIncr", dfSalesOrderIncr)
    val dfSalesOrderItemIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ITEM, mode, incrDate)
    dfMap.put("salesOrderItemIncr", dfSalesOrderItemIncr)
    val dfSalesOrderAddressFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ADDRESS, DataSets.FULL_MERGE_MODE, incrDate)
    dfMap.put("salesOrderAddressFull", dfSalesOrderAddressFull)
    val dfItrSkuSimple = CampaignInput.loadYesterdayItrSimpleData(incrDate)
    dfMap.put("itrSkuSimple", dfItrSkuSimple)

    dfMap
  }

  override def write(dfWrite: mutable.HashMap[String, DataFrame], saveMode: String, incrDate: String) = {
    val dfCityWiseMapData = dfWrite("cityWiseMapOut")
    logger.info("writing city Wise Map Data")
    val writeOutPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CITY_WISE_DATA, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(dfCityWiseMapData, writeOutPath, saveMode)
  }

  override def process(dfMap: mutable.HashMap[String, DataFrame]): mutable.HashMap[String, DataFrame] = {

    val dfSalesOrderIncr = dfMap("salesOrderIncr")
    val dfSalesOrderItemIncr = dfMap("salesOrderItemIncr")
    val dfSalesOrderAddressFull = dfMap("salesOrderAddressFull")
    val dfItrSkuSimple = dfMap("itrSkuSimple")

    val writeMap = new HashMap[String, DataFrame]()

    val salesJoinedData = dfSalesOrderIncr.join(dfSalesOrderItemIncr, dfSalesOrderIncr(SalesOrderVariables.ID_SALES_ORDER) ===
      dfSalesOrderItemIncr(SalesOrderItemVariables.FK_SALES_ORDER), SQL.INNER).join(dfSalesOrderAddressFull, dfSalesOrderIncr(SalesOrderVariables.FK_SALES_ORDER_ADDRESS_SHIPPING)
      === dfSalesOrderAddressFull(SalesAddressVariables.ID_SALES_ORDER_ADDRESS), SQL.INNER)
      .select(dfSalesOrderAddressFull(SalesAddressVariables.CITY),
        dfSalesOrderItemIncr(SalesOrderItemVariables.SKU))

    val salesWithItrData = salesJoinedData.join(dfItrSkuSimple, salesJoinedData(SalesOrderItemVariables.SKU) === dfItrSkuSimple(ProductVariables.SKU_SIMPLE), SQL.INNER)
      .select(salesJoinedData(SalesAddressVariables.CITY),
        dfItrSkuSimple(ProductVariables.BRAND),
        dfItrSkuSimple(ProductVariables.BRICK),
        dfItrSkuSimple(ProductVariables.GENDER),
        dfItrSkuSimple(ProductVariables.MVP),
        dfItrSkuSimple(ProductVariables.SPECIAL_PRICE))

    val pivotFields = Array(SalesAddressVariables.CITY)
    val attributeFields = Array(ProductVariables.BRAND, ProductVariables.BRICK, ProductVariables.GENDER, ProductVariables.MVP)
    val valueFields = Array("count", "sum_price")
    val cityWiseMapData = Utils.generateTopMap(salesWithItrData, pivotFields, attributeFields, valueFields, OrderBySchema.cityMapSchema)
    val dfCityWisePrevFull = dfMap.getOrElse("cityWisePrevFullData", null)
    if (dfCityWisePrevFull != null) {
      val cityJoinedData = dfCityWisePrevFull.join(cityWiseMapData, dfCityWisePrevFull(SalesAddressVariables.CITY) === cityWiseMapData(SalesAddressVariables.CITY), SQL.FULL_OUTER).rdd.map(row => Row(Utils.getNonNull(row(0), row(5)).asInstanceOf[String],
        Utils.mergeMaps(row(1).asInstanceOf[Map[String, Row]], row(6).asInstanceOf[Map[String, Row]]),
        Utils.mergeMaps(row(2).asInstanceOf[Map[String, Row]], row(7).asInstanceOf[Map[String, Row]]),
        Utils.mergeMaps(row(3).asInstanceOf[Map[String, Row]], row(8).asInstanceOf[Map[String, Row]]),
        Utils.mergeMaps(row(4).asInstanceOf[Map[String, Row]], row(9).asInstanceOf[Map[String, Row]])))
      //            .select(coalesce(dfCityWisePrevFull(SalesAddressVariables.CITY),cityWiseMapData(SalesAddressVariables.CITY)) as SalesAddressVariables.CITY,
      //                    Udf.mergeMap(dfCityWisePrevFull("brand_list"),cityWiseMapData("brand_list")) as "brand_list",
      //                    Udf.mergeMap(dfCityWisePrevFull("brick_list"),cityWiseMapData("brick_list")) as "brick_list",
      //                    Udf.mergeMap(dfCityWisePrevFull("gender_list"),cityWiseMapData("gender_list")) as "gender_list",
      //                    Udf.mergeMap(dfCityWisePrevFull("mvp_list"),cityWiseMapData("brand_list")) as "mvp_list")
      val dfCityData = sqlContext.createDataFrame(cityJoinedData, OrderBySchema.cityMapSchema)
      writeMap.put("cityWiseMapOut", dfCityData)
    } else {
      writeMap.put("cityWiseMapOut", cityWiseMapData)
    }

    return writeMap
  }
}
