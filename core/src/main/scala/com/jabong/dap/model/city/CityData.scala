package com.jabong.dap.model.city

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.Utils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ ProductVariables, SalesAddressVariables, SalesOrderItemVariables, SalesOrderVariables }
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.{ OrderBySchema, Schema }
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.dataFeeds.DataFeedsModel
import com.jabong.dap.model.order.variables.SalesOrder
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
 * Created by rahul on 16/11/15.
 */
object CityData extends DataFeedsModel with Logging {

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
    val dfSalesOrderAddressIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ADDRESS, mode, incrDate)
    dfMap.put("salesOrderAddressIncr", dfSalesOrderAddressIncr)
    val dfItrSkuSimple = CampaignInput.loadYesterdayItrSimpleData(incrDate)
    dfMap.put("itrSkuSimple", dfItrSkuSimple)

    dfMap
  }

  override def write(dfWrite: mutable.HashMap[String, DataFrame], saveMode: String, incrDate: String): Unit = {
    val dfCityWiseMapData = dfWrite("cityWiseMapOut")
    logger.info("writing city Wise Map Data")
    val writeOutPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CITY_WISE_DATA, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(dfCityWiseMapData, writeOutPath, saveMode)
  }

  override def process(dfMap: mutable.HashMap[String, DataFrame]): mutable.HashMap[String, DataFrame] = {

    val dfSalesOrderIncr = dfMap("salesOrderIncr")
    val dfSalesOrderItemIncr = dfMap("salesOrderItemIncr")
    val dfSalesOrderAddressIncr = dfMap("salesOrderAddressIncr")
    val dfItrSkuSimple = dfMap("itrSkuSimple")

    val salesJoinedData = dfSalesOrderIncr.join(dfSalesOrderItemIncr, dfSalesOrderIncr(SalesOrderVariables.ID_SALES_ORDER) ===
      dfSalesOrderItemIncr(SalesOrderItemVariables.FK_SALES_ORDER), SQL.INNER).join(dfSalesOrderAddressIncr, dfSalesOrderIncr(SalesOrderVariables.FK_SALES_ORDER_ADDRESS_SHIPPING)
      === dfSalesOrderAddressIncr(SalesAddressVariables.ID_SALES_ORDER_ADDRESS), SQL.INNER)
      .select(dfSalesOrderAddressIncr(SalesAddressVariables.CITY),
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
    //    if(dfCityWisePrevFull != null){
    //      cityWiseMapData.
    //    }
    val writeMap = new HashMap[String, DataFrame]()

    writeMap.put("cityWiseMapOut", cityWiseMapData)
    return writeMap
  }
}
