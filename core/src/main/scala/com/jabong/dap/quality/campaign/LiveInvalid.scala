package com.jabong.dap.quality.campaign


import com.jabong.dap.common.constants.campaign.CampaignMergedFields
import com.jabong.dap.common.constants.variables.{SalesOrderItemVariables, SalesOrderVariables}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by jabong on 12/8/15.
 */
object LiveInvalid {

  def isInvalidOrder(sales: DataFrame, campaign: DataFrame): Boolean ={
    val joined = sales.join(campaign, sales(SalesOrderVariables.FK_CUSTOMER) === campaign(CampaignMergedFields.CUSTOMER_ID))
                    .select(CampaignMergedFields.CUSTOMER_ID,
                            SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS, SalesOrderItemVariables.UPDATED_AT)

    return false
  }

  def isLowStock(itr: DataFrame, campaign: DataFrame): Boolean={

    return false
  }


}
