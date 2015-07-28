package com.jabong.dap.campaign.campaignlist

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.common.constants.campaign.{ SkuSelection, CustomerSelection, CampaignCommon }
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 28/7/15.
 */
object SurfCampaign {

  def runCampaign(): Unit = {
    
    // surf 1,2,6
    // one day surf session data
    val yestSurfSessionData = CampaignInput.loadYesterdaySurfSessionData()

    // not bought for last day
    val yestOrderItemData = CampaignInput.loadYesterdayOrderItemData()
    val fullOrderData = CampaignInput.loadFullOrderData()
    val yestOrderData = CampaignInput.loadLastNdaysOrderData(1, fullOrderData)
    
    val yestItrSkuData = CampaignInput.loadYesterdayItrSkuData()
    
    // load customer master record for email id to fk_customer mapping
    val customerMasterData = CampaignInput.loadCustomerMasterData()
    
    // common customer selection for surf 1, 2, 6
    val surf1Campaign = new Surf1Campaign()
    surf1Campaign.runCampaign(yestSurfSessionData, yestItrSkuData, customerMasterData, yestOrderData, yestOrderItemData)
    
    /*
    val customerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR).getCustomerSelector(CustomerSelection.YESTERDAY_SESSION)

    val customerSurfData = customerSelector.customerSelection(null)

    val dfSkuSelector = CampaignProducer.getFactory(CampaignCommon.SKU_SELECTOR).getSkuSelector(SkuSelection.SURF)

    val skuSelector = dfSkuSelector.skuFilter(dfCustomerPageVisit, dfItrData, dfCustomer, dfSalesOrder, dfSalesOrderItem)
    */
  }

}
