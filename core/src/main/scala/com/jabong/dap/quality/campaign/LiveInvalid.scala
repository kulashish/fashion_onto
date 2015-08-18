package com.jabong.dap.quality.campaign


import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.campaign.{CampaignCommon, CampaignMergedFields}
import com.jabong.dap.common.constants.status.OrderStatus
import com.jabong.dap.common.constants.variables.{SalesOrderItemVariables, SalesOrderVariables}
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.product.itr.variables.ITR
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by jabong on 12/8/15.
 */
object LiveInvalid {


  /**
   *
   * @param sales
   * @param campaign
   * @return
   */
  def checkInvalidOrder(sales: DataFrame, campaign: DataFrame): Boolean ={
    val joined = sales.join(campaign, sales(SalesOrderVariables.FK_CUSTOMER) === campaign(CampaignMergedFields.CUSTOMER_ID) &&
      sales(SalesOrderItemVariables.SKU) === campaign(CampaignMergedFields.REF_SKU1) )
                    .select(CampaignMergedFields.CUSTOMER_ID,
                            SalesOrderItemVariables.FK_SALES_ORDER_ITEM_STATUS,
                            SalesOrderItemVariables.CREATED_AT).orderBy(desc(SalesOrderItemVariables.CREATED_AT))
    //TODO check whether to use updated_at or created_at
    val grouped = joined.groupBy(CampaignMergedFields.CUSTOMER_ID).agg(first(SalesOrderItemVariables.FK_SALES_ORDER_ITEM_STATUS) as "status")

    val fil = grouped.filter(grouped("status").!==(OrderStatus.CANCEL_PAYMENT_ERROR) || grouped("status").!==(OrderStatus.INVALID))
    return fil.count() == 0
    //TODO call the lowstock and followup methods from SKUSelectionQuality
  }



}
