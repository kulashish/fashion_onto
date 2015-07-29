package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ SkuSelection, CampaignCommon }
import com.jabong.dap.common.time.TimeConstants
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul for return reTarget campaign on 7/7/15.
 */
class LiveReturnReTargetCampaign {

  def runCampaign(targetCustomersWithOrderItems: DataFrame): Unit = {

    // filter only by return status
    // filter by campaign -- i.e., campaign shount not have gone to customer in last 30 days

    // find list of (customers, ref skus)
    val cancelRetargetSkuSelector = CampaignProducer.getFactory(CampaignCommon.SKU_SELECTOR).getSkuSelector(SkuSelection.RETURN_RETARGET)
    val refSkus = cancelRetargetSkuSelector.skuFilter(targetCustomersWithOrderItems)

    // create recommendations
    //  val recommender = CampaignProducer.getFactory(CampaignCommon.RECOMMENDER).getRecommender("Null")
    // val recommendations = recommender.recommend(refSkus)

    // save 2 ref skus + 8 recommendation per customer (null allowed for mobile push)
    CampaignOutput.saveCampaignData(refSkus, CampaignCommon.BASE_PATH + "/" + CampaignCommon.RETURN_RETARGET_CAMPAIGN + "/" + CampaignUtils.now(TimeConstants.DATE_FORMAT_FOLDER))

    //    returnCancelCustomer.customerSelection()

  }
}
