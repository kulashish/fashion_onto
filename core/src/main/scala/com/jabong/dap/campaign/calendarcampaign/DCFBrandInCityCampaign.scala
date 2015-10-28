package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CustomerSelection }
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 14/9/15.
 */
class DCFBrandInCityCampaign {

  def runCampaign(yestCustomerData: DataFrame, last6thDaySalesOrderData: DataFrame, last6thDaySalesOrderItemData: DataFrame, recommendationsData: DataFrame) = {

    val customerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.CUSTOMER_PREFERRED_DATA)

    val dfCustomerSelection = customerSelector.customerSelection(yestCustomerData, last6thDaySalesOrderData, last6thDaySalesOrderItemData)

    //TODO: Generate reference sku and recommendation

  }

}
