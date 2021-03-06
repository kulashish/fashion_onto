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

  def writeRecommendation(recommendedOutput: DataFrame, recommendationType: String) = {
    val outPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.RECOMMENDATIONS, recommendationType, DataSets.DAILY_MODE, TimeUtils.YESTERDAY_FOLDER)
    if (DataWriter.canWrite(DataSets.IGNORE_SAVEMODE, outPath))
      DataWriter.writeParquet(recommendedOutput, outPath, DataSets.IGNORE_SAVEMODE)
  }
}
