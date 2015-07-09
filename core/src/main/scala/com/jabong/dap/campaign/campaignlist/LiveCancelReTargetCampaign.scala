package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import org.apache.spark.sql.DataFrame

/**
 * Created by jabong1145 on 7/7/15.
 */
class LiveCancelReTargetCampaign {

  def runCampaign(targetCustomersWithOrderItems: DataFrame): Unit ={

    // filter only by return status
    // filter by campaign -- i.e., campaign shount not have gone to customer in last 30 days
    
    // find list of (customers, ref skus)
    val cancelRetargetSkuSelector = CampaignProducer.getFactory("SkuSelection").getSkuSelector("CancelReTarget")
    val refSkus = cancelRetargetSkuSelector.skuFilter(targetCustomersWithOrderItems)

    // create recommendations
    val recommender = CampaignProducer.getFactory("Recommendation").getRecommender("Null")
    val recommendations = recommender.recommend(refSkus)
    
    // save 2 ref skus + 8 recommendation per customer (null allowed for mobile push)
    CampaignOutput.saveCampaignData(recommendations)
    
    
//    returnCancelCustomer.customerSelection()

  }

}
