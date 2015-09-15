package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.data.CampaignOutput
import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.DCFBrandInCity
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ SkuSelection, CustomerSelection, CampaignCommon }
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 14/9/15.
 */
class DCFBrandInCityCampaign {

  def runCampaign(yestCustomerData: DataFrame, last6thDaySalesOrderData: DataFrame, last6thDaySalesOrderItemData: DataFrame, recommendationsData: DataFrame) = {

    val skus = DCFBrandInCity.skuFilter(yestCustomerData, last6thDaySalesOrderData, last6thDaySalesOrderItemData)

    //TODO: Generate reference sku and recommendation

  }

}
