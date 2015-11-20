package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.Daily
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.campaign.{ Recommendation, CampaignMergedFields, CampaignCommon, CustomerSelection }
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Created by raghu on 14/9/15.
 */
class BrandInCityCampaign {

  def runCampaign(fullCustomerOrders: DataFrame, last6thDaySalesOrderData: DataFrame, last6thDaySalesOrderItemData: DataFrame, brandMvpCityRecommendations: DataFrame, yesterdayItrData: DataFrame, incrDate: String) = {

    val customerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.CUSTOMER_PREFERRED_DATA)

    val dfCustomerSelection = customerSelector.customerSelection(fullCustomerOrders, last6thDaySalesOrderData, last6thDaySalesOrderItemData)

    CampaignUtils.debug(dfCustomerSelection, "after dfCustomerSelection")

    val filteredSku = Daily.skuFilter(dfCustomerSelection, yesterdayItrData)
      .select(
        col(CustomerVariables.FK_CUSTOMER),
        col(CustomerVariables.EMAIL),
        Udf.toLowercase(col(SalesAddressVariables.CITY)) as SalesAddressVariables.CITY,
        col(CustomerVariables.PREFERRED_BRAND),
        col(ProductVariables.SKU_SIMPLE),
        col(ProductVariables.SPECIAL_PRICE),
        col(ProductVariables.BRICK),
        col(ProductVariables.BRAND),
        col(ProductVariables.MVP),
        col(ProductVariables.GENDER),
        col(ProductVariables.PRODUCT_NAME),
        col(ProductVariables.STOCK),
        col(ProductVariables.PRICE_BAND)
      )

    val dfBrandMvpCityRecommendations = brandMvpCityRecommendations.select(
      brandMvpCityRecommendations(ProductVariables.BRAND),
      brandMvpCityRecommendations(ProductVariables.MVP),
      Udf.toLowercase(brandMvpCityRecommendations(SalesAddressVariables.CITY)) as SalesAddressVariables.CITY,
      brandMvpCityRecommendations(ProductVariables.GENDER),
      brandMvpCityRecommendations(CampaignMergedFields.RECOMMENDATIONS)
    )

    CampaignUtils.debug(filteredSku, "after filteredSku")

    //    val limitOnSkuFilterData = filteredSku.limit(1000)
    // ***** email use case
    CampaignUtils.campaignPostProcess(DataSets.CALENDAR_CAMPAIGNS, CampaignCommon.BRAND_IN_CITY_CAMPAIGN, filteredSku, false, dfBrandMvpCityRecommendations)

  }

}
