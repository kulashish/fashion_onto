package com.jabong.dap.campaign.actions

import org.apache.spark.sql.DataFrame

/**
 * Created by jabong1145 on 6/7/15.
 */
class FollowUp extends Action{

/*
Place Holder for Followup Action
 */
  override def execute(inputDataFrame: DataFrame): DataFrame = {
    return null
  }

  override def skuFilter(inDataFrame: DataFrame): DataFrame = ???
}
