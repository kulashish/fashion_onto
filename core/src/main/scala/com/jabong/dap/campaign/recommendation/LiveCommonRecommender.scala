package com.jabong.dap.campaign.recommendation

import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.ProductVariables
import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.types._

/**
 * Created by jabong1145 on 21/8/15.
 */
class LiveCommonRecommender extends BasicRecommender {

  override def generateRecommendation(orderItemFullData: DataFrame,yesterdayItr: DataFrame): DataFrame = {
    val dataFrameSchema = StructType(Array(
      StructField(ProductVariables.BRICK, StringType, false),
      StructField(ProductVariables.MVP, LongType, false),
      StructField(ProductVariables.GENDER, StringType, false),
      StructField(ProductVariables.RECOMMENDATIONS, ArrayType(StructType(Array(StructField(ProductVariables.QUANTITY, LongType), StructField(ProductVariables.SKU_LIST, StringType))), true))
    ))
    val pivotKeys = Array(ProductVariables.BRICK, ProductVariables.MVP)
    val topProducts = topProductsSold(orderItemFullData, 30)
    val skuCompleteData = skuCompleteData(topProducts,yesterdayItr)
    val recommendedSkus = genRecommend(skuCompleteData,pivotKeys,dataFrameSchema)
    val outPath = DataWriter.getWritePath(ConfigConstants.OUTPUT_PATH,DataSets.RECOMMENDATIONS,DataSets.BRICK_MVP_RECOMMENDATIONS,DataSets.DAILY_MODE,TimeUtils.YESTERDAY_FOLDER)
    DataWriter.writeParquet(recommendedSkus,outPath,DataSets.ERROR_SAVEMODE)
    return recommendedSkus
  }
}
