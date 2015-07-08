package com.jabong.dap.campaign.utils

import com.jabong.dap.common.constants.variables.{ProductVariables, CustomerVariables}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by jabong1145 on 15/6/15.
 */
object CampaignUtils {

  def generateReferenceSku(skuData:DataFrame,NumberSku:Int): DataFrame ={
    val customerRefSku = skuData.groupBy(CustomerVariables.FK_CUSTOMER).agg(first(ProductVariables.SKU))
    return customerRefSku

  }


}
