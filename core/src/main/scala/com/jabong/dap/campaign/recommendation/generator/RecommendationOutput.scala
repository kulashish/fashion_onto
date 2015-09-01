package com.jabong.dap.campaign.recommendation.generator

import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import org.apache.spark.sql.DataFrame

/**
 * Created by jabong1145 on 27/8/15.
 */
object RecommendationOutput {

  def writeRecommendation(recommendedOutput: DataFrame): Unit ={
    val outPath = DataWriter.getWritePath(ConfigConstants.OUTPUT_PATH,DataSets.RECOMMENDATIONS,DataSets.BRICK_MVP_RECOMMENDATIONS,DataSets.DAILY_MODE,TimeUtils.YESTERDAY_FOLDER)
    if (DataWriter.canWrite(DataSets.IGNORE_SAVEMODE, outPath))
      DataWriter.writeParquet(recommendedOutput,outPath,DataSets.IGNORE_SAVEMODE)
  }
}
