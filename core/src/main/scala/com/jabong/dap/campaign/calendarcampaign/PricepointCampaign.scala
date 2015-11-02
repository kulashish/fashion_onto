package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.Daily
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CustomerSelection }
import com.jabong.dap.common.constants.variables.{ ProductVariables, CustomerVariables, SalesOrderItemVariables, SalesOrderVariables }
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 16/9/15.
 */
class PricepointCampaign {

  def runCampaign(last20thDaySalesOrderData: DataFrame, last20thDaySalesOrderItemData: DataFrame, brickPriceBandRecommendations: DataFrame, yesterdayItrData: DataFrame) = {

    val customerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.LAST_ORDER)

    val dfCustomerSelection = customerSelector.customerSelection(last20thDaySalesOrderData, last20thDaySalesOrderItemData)

    //filter sku based on daily filter
    val filteredSku = Daily.skuFilter(dfCustomerSelection, yesterdayItrData)

    val dfNextPriceBand = filteredSku.select(
      col(SalesOrderVariables.FK_CUSTOMER),
      col(CustomerVariables.EMAIL),
      col(ProductVariables.SKU_SIMPLE),
      col(ProductVariables.SPECIAL_PRICE),
      col(ProductVariables.BRICK),
      col(ProductVariables.BRAND),
      col(ProductVariables.MVP),
      col(ProductVariables.GENDER),
      col(ProductVariables.PRODUCT_NAME),
      Udf.nextPriceBand(col(ProductVariables.PRICE_BAND)) as ProductVariables.PRICE_BAND
    )
    // ***** email use case
    CampaignUtils.campaignPostProcess(DataSets.CALENDAR_CAMPAIGNS, CampaignCommon.PRICEPOINT_CAMPAIGN, dfNextPriceBand, false, brickPriceBandRecommendations)

  }

}
