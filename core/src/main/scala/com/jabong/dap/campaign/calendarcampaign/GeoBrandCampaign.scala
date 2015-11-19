package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.{ CustomerSelection, CampaignCommon }
import com.jabong.dap.common.constants.variables.{ SalesAddressVariables, ProductVariables, CustomerVariables }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by samathashetty on 19/11/15.
 */
class GeoBrandCampaign {

  def runCampaign(day50_SalesOrder: DataFrame, day50_SalesOrderItem: DataFrame, salesAddressData: DataFrame, yesterdayItrData: DataFrame, cityWiseData: DataFrame,
                  recommendationsData: DataFrame) = {

    val customerSelection = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR).getCustomerSelector(CustomerSelection.LAST_ORDER)

    val selectCustomers = customerSelection.customerSelection(day50_SalesOrder, day50_SalesOrderItem, salesAddressData)

    val cityWiseMap = CampaignUtils.getFavouriteAttribute(cityWiseData, CustomerVariables.CITY, ProductVariables.BRAND, 1)

    val selectCustCity = selectCustomers.join(cityWiseMap, selectCustomers(SalesAddressVariables.CITY) === cityWiseMap(CustomerVariables.CITY), SQL.LEFT_OUTER).
      select(selectCustomers("*"),
        cityWiseMap(ProductVariables.BRAND))

    //Note: Not using the Daily filter intentionally to use the BRAND from the city map
    val custFilter = selectCustCity.join(yesterdayItrData, selectCustCity(ProductVariables.SKU_SIMPLE) === yesterdayItrData(ProductVariables.SKU_SIMPLE), SQL.INNER)
      .select(selectCustCity("*"),
        yesterdayItrData(ProductVariables.BRICK),
        yesterdayItrData(ProductVariables.SPECIAL_PRICE),
        yesterdayItrData(ProductVariables.MVP),
        yesterdayItrData(ProductVariables.GENDER),
        yesterdayItrData(ProductVariables.PRODUCT_NAME),
        yesterdayItrData(ProductVariables.STOCK),
        yesterdayItrData(ProductVariables.PRICE_BAND))

    CampaignUtils.campaignPostProcess(DataSets.CALENDAR_CAMPAIGNS, CampaignCommon.GEO_BRAND_CAMPAIGN, custFilter, false, recommendationsData)

  }

}
