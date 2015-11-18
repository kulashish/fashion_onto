package com.jabong.dap.model.customer.data

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.Utils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.dataFeeds.DataFeedsModel
import com.jabong.dap.model.product.itr.variables.ITR
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

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

    val dfPageViewSurfData = CampaignInput.loadPageViewSurfData(incrDate)
    dfMap.put("dfPageViewSurfData", dfPageViewSurfData)

    val yestItr = CampaignInput.loadYesterdayItrSkuData(incrDate)
    dfMap.put("yestItr", yestItr)

    if (null == paths) {
      val dfSurfAffinityFull = DataReader.getDataFrame(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_SURF_AFFINITY, DataSets.FULL_MERGE_MODE, prevDate)
      dfMap.put("dfSurfAffinityPrevFull", dfSurfAffinityFull)
    }
    dfMap
  }

  def process(dfMap: HashMap[String, DataFrame]): HashMap[String, DataFrame] = {

    val dfPageViewSurfData = dfMap("dfPageViewSurfData")
    val yestItr = dfMap("yestItr")
    val dfSurfAffinityPrevFull = dfMap.getOrElse("dfSurfAffinityPrevFull", null)

    val dfSurfAffinityInc = getSurfAffinity(dfPageViewSurfData, yestItr)

    val dfWriteMap = new HashMap[String, DataFrame]()
    if (dfSurfAffinityPrevFull != null) {
      val dfMergedTopMapDataFrame = Utils.mergeTopMapDataFrame(dfSurfAffinityPrevFull, dfSurfAffinityInc, CustomerVariables.EMAIL, Schema.surfAffinitySchema)
      dfWriteMap.put("dfSurfAffinityFull", dfMergedTopMapDataFrame)
    } else {
      dfWriteMap.put("dfSurfAffinityFull", dfSurfAffinityInc)
    }

    dfWriteMap
  }

  def getSurfAffinity(dfPageViewSurfData: DataFrame, yestItr: DataFrame): DataFrame = {

    val filteredSurfData = dfPageViewSurfData.filter(!(col(PageVisitVariables.USER_ID).isNull || (col(PageVisitVariables.USER_ID).startsWith(CustomerVariables.APP_FILTER))))
      .select(
        col(PageVisitVariables.USER_ID),
        col(PageVisitVariables.PRODUCT_SKU) as SalesOrderItemVariables.SKU
      )

    val joinedItr = filteredSurfData.join(yestItr, filteredSurfData(SalesOrderItemVariables.SKU) === yestItr(ProductVariables.SKU), SQL.INNER)
      .select(filteredSurfData(PageVisitVariables.USER_ID) as CustomerVariables.EMAIL,
        yestItr(ProductVariables.BRAND),
        yestItr(ProductVariables.BRICK),
        yestItr(ProductVariables.GENDER),
        yestItr(ProductVariables.MVP),
        yestItr(ProductVariables.SPECIAL_PRICE)
      )

    val groupFields = Array(CustomerVariables.EMAIL)
    val attributeFields = Array(ProductVariables.BRAND, ProductVariables.BRICK, ProductVariables.GENDER, ProductVariables.MVP)
    val valueFields = Array("count", "sum_price")

    val surfAffinityData = Utils.generateTopMap(joinedItr, groupFields, attributeFields, valueFields, Schema.surfAffinitySchema)

    return surfAffinityData
  }

  def write(dfWrite: HashMap[String, DataFrame], saveMode: String, incrDate: String) = {
    val pathSurfAffinityFull = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUSTOMER_SURF_AFFINITY, DataSets.FULL_MERGE_MODE, incrDate)
    if (DataWriter.canWrite(saveMode, pathSurfAffinityFull)) {
      DataWriter.writeParquet(dfWrite("dfSurfAffinityFull"), pathSurfAffinityFull, saveMode)
    }
  }

}

