package com.jabong.dap.quality.campaign

import com.jabong.dap.common.constants.campaign.{CampaignCommon, CampaignMergedFields}
import com.jabong.dap.common.constants.variables.ProductVariables
import com.jabong.dap.model.product.itr.variables.ITR
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul on 17/8/15.
 */
object SkuSelectionQuality {


  def validateLowStock(itr: DataFrame, campaign: DataFrame): Boolean={
    val joined = itr.join(campaign, itr(ProductVariables.SKU_SIMPLE) === campaign(CampaignMergedFields.REF_SKU1))
      .select(CampaignMergedFields.REF_SKU1,
        ITR.QUANTITY)
    joined.rdd.foreach(e => if(Integer.parseInt(e(1).toString)> CampaignCommon.LOW_STOCK_VALUE){
      return false
    })
    return true
  }


  def validateFollowupStock(itr: DataFrame, campaign: DataFrame): Boolean={
    val joined = itr.join(campaign, itr(ProductVariables.SKU_SIMPLE) === campaign(CampaignMergedFields.REF_SKU1))
      .select(CampaignMergedFields.REF_SKU1,
        ITR.QUANTITY)
    joined.rdd.foreach(e => if(Integer.parseInt(e(1).toString)<= CampaignCommon.FOLLOW_UP_STOCK_VALUE){
      return false
    })
    return true
  }


}
