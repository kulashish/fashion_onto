package com.jabong.dap.model.customer.data

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.Utils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.dataFeeds.DataFeedsModel
import com.jabong.dap.model.product.itr.variables.ITR
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.collection.mutable.HashMap

/**
 * Created by raghu on 17/11/15.
 */
object CustomerSurfAffinity extends DataFeedsModel {

  def canProcess(incrDate: String, saveMode: String): Boolean = {
    val pathSurfAffinityFull = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_SURF_AFFINITY, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.canWrite(saveMode, pathSurfAffinityFull)
  }

  /**
   *
   * @param paths
   * @param incrDate
   * @param prevDate
   * @return
   */
  def readDF(incrDate: String, prevDate: String, paths: String): HashMap[String, DataFrame] = {
    val dfMap = new HashMap[String, DataFrame]()

    val dfSurfData = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.CLICKSTREAM, DataSets.SURF1_PROCESSED_VARIABLE, DataSets.DAILY_MODE, incrDate)
    dfMap.put("dfSurfData", dfSurfData)
    val yestItr = CampaignInput.loadYesterdayItrSkuData(incrDate)
    dfMap.put("yestItr", yestItr)

    if (null == paths) {
      val dfSurfAffinityFull = DataReader.getDataFrame(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_SURF_AFFINITY, DataSets.FULL_MERGE_MODE, prevDate)
      dfMap.put("dfSurfAffinityPrevFull", dfSurfAffinityFull)
    }
    dfMap
  }

  def process(dfMap: HashMap[String, DataFrame]): HashMap[String, DataFrame] = {

    val dfSurfData = dfMap("dfSurfData")
    val yestItr = dfMap("yestItr")
    val dfSurfAffinityInc = getSurfAffinity(dfSurfData, yestItr)
    val dfSurfAffinityFull = dfSurfAffinityInc

    val dfSurfAffinityPrevFull = dfMap.getOrElse("dfSurfAffinityPrevFull", null)

    //FIXME: Add merge logic here
    if (null != dfSurfAffinityPrevFull) {

    }
    val dfWrite = new HashMap[String, DataFrame]()
    dfWrite.put("dfSurfAffinityInc", dfSurfAffinityInc)
    dfWrite.put("dfSurfAffinityFull", dfSurfAffinityFull)

    dfWrite
  }

  def getSurfAffinity(dfSurfData: DataFrame, yestItr: DataFrame): DataFrame = {

    val filteredSurfData = dfSurfData.filter(!(col(PageVisitVariables.USER_ID).isNull || (col(PageVisitVariables.USER_ID).startsWith(CustomerVariables.APP_FILTER)))).select(PageVisitVariables.USER_ID, PageVisitVariables.SKU_LIST)

    val dfRepeatedSku = filteredSurfData.select(
      col(PageVisitVariables.USER_ID),
      explode(Udf.repeatedSku(col(PageVisitVariables.SKU_LIST))) as PageVisitVariables.SKU
    )

    //FIXME: remove .na.fill("") once it will fix on Utils
    val joinedItr = dfRepeatedSku.join(yestItr, dfRepeatedSku(SalesOrderItemVariables.SKU) === yestItr(ProductVariables.SKU), SQL.INNER)
      .select(dfRepeatedSku(PageVisitVariables.USER_ID) as CustomerVariables.EMAIL,
        yestItr(ProductVariables.BRAND),
        yestItr(ProductVariables.BRICK),
        yestItr(ProductVariables.GENDER),
        yestItr(ProductVariables.MVP),
        yestItr(ProductVariables.SPECIAL_PRICE)
      ).
        na.fill(Map(
          ProductVariables.BRAND -> "",
          ProductVariables.BRICK -> "",
          ProductVariables.GENDER -> "",
          ProductVariables.MVP -> "",
          ITR.SPECIAL_PRICE -> 0.00
        ))

    val groupFields = Array(CustomerVariables.EMAIL)
    val attributeFields = Array(ProductVariables.BRAND, ProductVariables.BRICK, ProductVariables.GENDER, ProductVariables.MVP)
    val valueFields = Array("count", "sum_price")

    val surfAffinityData = Utils.generateTopMap(joinedItr, groupFields, attributeFields, valueFields, Schema.surfAffinitySchema)

    return surfAffinityData
  }

  def write(dfWrite: HashMap[String, DataFrame], saveMode: String, incrDate: String) = {
    val pathSurfAffinityInc = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_SURF_AFFINITY, DataSets.DAILY_MODE, incrDate)
    val pathSurfAffinityFull = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_SURF_AFFINITY, DataSets.FULL_MERGE_MODE, incrDate)
    if (DataWriter.canWrite(saveMode, pathSurfAffinityFull)) {
      DataWriter.writeParquet(dfWrite("dfSurfAffinityInc"), pathSurfAffinityInc, saveMode)
      DataWriter.writeParquet(dfWrite("dfSurfAffinityFull"), pathSurfAffinityFull, saveMode)
    }
  }

}

