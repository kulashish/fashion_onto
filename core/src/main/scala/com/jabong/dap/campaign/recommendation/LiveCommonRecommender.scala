package com.jabong.dap.campaign.recommendation

import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.ProductVariables
import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import grizzled.slf4j.Logging
import org.apache.spark.sql.{ DataFrame }
import org.apache.spark.sql.types._

/**
 * Created by rahul aneja on 21/8/15.
 */
class LiveCommonRecommender extends Recommender with Logging {
  /**
   * Place holder function which will get recommended skus
   * @param refSkus
   * @param yesterdayItr
   * @return
   */
  override def generateRecommendation(refSkus: DataFrame, yesterdayItr: DataFrame): DataFrame = {
    return null
  }
}
