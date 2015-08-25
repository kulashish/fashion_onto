package com.jabong.dap.campaign.recommendation

import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.ProductVariables
import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import grizzled.slf4j.Logging
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.types._

/**
 * Created by rahul aneja on 21/8/15.
 */
class LiveCommonRecommender extends BasicRecommender with Logging {

  override def generateRecommendation(orderItemFullData: DataFrame,yesterdayItr: DataFrame): DataFrame = {
    val dataFrameSchema = StructType(Array(
      StructField(ProductVariables.BRICK, StringType, false),
      StructField(ProductVariables.MVP, LongType, false),
      StructField(ProductVariables.GENDER, StringType, false),
      StructField(ProductVariables.RECOMMENDATIONS, ArrayType(StructType(Array(StructField(ProductVariables.QUANTITY, LongType), StructField(ProductVariables.SKU_LIST, StringType))), true))
    ))
    val pivotKeys = Array(ProductVariables.BRICK, ProductVariables.MVP)
    val topProducts = topProductsSold(orderItemFullData, 30)
    println("top products count:-" +topProducts.count)
    val skuData = skuCompleteData(topProducts,yesterdayItr)
    println("sku complete data:-" +skuData.count)
    val recommendedSkus = genRecommend(skuData,pivotKeys,dataFrameSchema)
    println("rec skus data:-" +recommendedSkus.count)
    val outPath = DataWriter.getWritePath(ConfigConstants.OUTPUT_PATH,DataSets.RECOMMENDATIONS,DataSets.BRICK_MVP_RECOMMENDATIONS,DataSets.DAILY_MODE,TimeUtils.YESTERDAY_FOLDER)
    DataWriter.writeParquet(recommendedSkus,outPath,DataSets.IGNORE_SAVEMODE)
    return recommendedSkus
  }
}
