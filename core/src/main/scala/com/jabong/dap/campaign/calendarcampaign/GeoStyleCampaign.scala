package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.{CampaignCommon, CustomerSelection}
import com.jabong.dap.common.constants.variables.{CustomerVariables, ProductVariables, SalesAddressVariables}
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by samathashetty on 17/11/15.
 */
class GeoStyleCampaign {
  def runCampaign(day50_SalesOrder: DataFrame, fullOrderItemData: DataFrame, salesAddressData: DataFrame, yesterdayItrData: DataFrame, cityWiseData: DataFrame,
                  recommendationsData: DataFrame) = {

    val customerSelection = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR).getCustomerSelector(CustomerSelection.LAST_ORDER)

    val selectCustomers = customerSelection.customerSelection(day50_SalesOrder, fullOrderItemData, salesAddressData)

    val cityWiseMap = CampaignUtils.getFavouriteAttribute(cityWiseData, CustomerVariables.CITY, ProductVariables.BRICK, 1)

    val selectCustCity = selectCustomers.join(cityWiseMap, selectCustomers(SalesAddressVariables.CITY) === cityWiseMap(CustomerVariables.CITY), SQL.LEFT_OUTER).
      select(selectCustomers("*"),
        cityWiseMap(ProductVariables.BRICK))

    //Note: Not using the Daily filter intentionally to use the BRIK from the city map
    val custFilter = selectCustCity.join(yesterdayItrData, selectCustCity(ProductVariables.SKU_SIMPLE) === yesterdayItrData(ProductVariables.SKU_SIMPLE), SQL.INNER)
      .select(selectCustCity("*"),
        yesterdayItrData(ProductVariables.SPECIAL_PRICE),
        yesterdayItrData(ProductVariables.BRAND),
        yesterdayItrData(ProductVariables.MVP),
        yesterdayItrData(ProductVariables.GENDER),
        yesterdayItrData(ProductVariables.PRODUCT_NAME),
        yesterdayItrData(ProductVariables.STOCK),
        yesterdayItrData(ProductVariables.PRICE_BAND))

    CampaignUtils.campaignPostProcess(DataSets.CALENDAR_CAMPAIGNS, CampaignCommon.GEO_STYLE, custFilter, false, recommendationsData)

  }


}


