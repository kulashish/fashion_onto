package com.jabong.dap.quality.campaign

import org.apache.spark.sql.DataFrame

/**
 * Created by Kapil.Rajak on 14/8/15.
 */
abstract class BaseCampaignQuality {

  /**
   *
   * @return campaign name
   */
  def getName(): String
  /**
   * gives random selected rows from DataFrame
   * @param df
   * @param fraction 1 means give all data, .5 means give half of the data selecting in random
   * @return
   */
  def getSample(df: DataFrame, fraction: Double): DataFrame = {
    df.sample(false, fraction)
  }

  def backwardTest(date: String, fraction: Double): Boolean

}
