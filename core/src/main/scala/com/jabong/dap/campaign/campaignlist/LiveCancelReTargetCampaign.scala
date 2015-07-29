package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, SkuSelection }
import com.jabong.dap.common.time.TimeConstants
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul for live cancel reTarget campaign on 7/7/15.
 */
class LiveCancelReTargetCampaign {

  def runCampaign(targetCustomersWithOrderItems: DataFrame): Unit = {
    // targetCustomersWithOrderItems = (id_customer, id_sales_order, item_status, unit_price, updated_at, sku_simple)

    // filter only by return status
    // FIXME: filter by campaign -- i.e., campaign shount not have gone to customer in last 30 days
    // find list of (customers, ref skus)
    val cancelRetargetSkuSelector = CampaignProducer.getFactory(CampaignCommon.SKU_SELECTOR).getSkuSelector(SkuSelection.CANCEL_RETARGET)
    val refSkus = cancelRetargetSkuSelector.skuFilter(targetCustomersWithOrderItems)

    // create recommendations
    //  val recommender = CampaignProducer.getFactory(CampaignCommon.RECOMMENDER).getRecommender("Null")
    // val recommendations = recommender.recommend(refSkus)

    // save 2 ref skus + 8 recommendation per customer (null allowed for mobile push)
    CampaignOutput.saveCampaignData(refSkus, CampaignCommon.BASE_PATH + "/" + CampaignCommon.CANCEL_RETARGET_CAMPAIGN + "/" + CampaignUtils.now(TimeConstants.DATE_FORMAT_FOLDER))

    //    returnCancelCustomer.customerSelection()

  }

}
