package com.jabong.dap.campaign.skuselection

import com.jabong.dap.campaign.utils.CampaignUtils
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 30/9/15.
 */
object CategoryReplenish extends Logging {

  def skuFilter(customerSkuData: DataFrame, yesterdayItrData: DataFrame): (DataFrame, DataFrame) = {

    if (customerSkuData == null || yesterdayItrData == null) {
      logger.error("either customer selected skus are null or itrData is null")
      return null
    }

    val filteredSkuJoinedItr = CampaignUtils.yesterdayItrJoin(customerSkuData, yesterdayItrData)

    //FIXME: generate data based on category

    return (null, null)
  }

}
