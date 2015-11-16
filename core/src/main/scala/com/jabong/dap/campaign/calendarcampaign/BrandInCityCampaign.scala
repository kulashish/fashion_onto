package com.jabong.dap.campaign.calendarcampaign

import com.jabong.dap.campaign.manager.CampaignProducer
import com.jabong.dap.campaign.skuselection.Daily
import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CustomerSelection }
import com.jabong.dap.common.constants.variables.{ ProductVariables, CustomerVariables, SalesOrderVariables }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 14/9/15.
 */
class BrandInCityCampaign {

  def runCampaign(fullCusTop5: DataFrame, fullSalesOrderAddress: DataFrame, last6thDaySalesOrderData: DataFrame, last6thDaySalesOrderItemData: DataFrame, brandMvpSubType: DataFrame, yesterdayItrData: DataFrame) = {

    val customerData = getCustomerData(fullCusTop5, fullSalesOrderAddress)

    val customerSelector = CampaignProducer.getFactory(CampaignCommon.CUSTOMER_SELECTOR)
      .getCustomerSelector(CustomerSelection.CUSTOMER_PREFERRED_DATA)

    val dfCustomerSelection = customerSelector.customerSelection(customerData, last6thDaySalesOrderData, last6thDaySalesOrderItemData)

    val filteredSku = Daily.skuFilter(dfCustomerSelection, yesterdayItrData)

    // ***** email use case
    CampaignUtils.campaignPostProcess(DataSets.CALENDAR_CAMPAIGNS, CampaignCommon.BRAND_IN_CITY_CAMPAIGN, filteredSku, false, brandMvpSubType)

  }

  def getCustomerData(dfCusTop5: DataFrame, dfSalesOrderAddress: DataFrame): DataFrame = {

    val cusTop5 = dfCusTop5.select(dfCusTop5(SalesOrderVariables.FK_CUSTOMER), dfCusTop5("BRAND_1") as ProductVariables.BRAND)

    val salesOrderAddress = dfSalesOrderAddress.select(SalesOrderVariables.FK_CUSTOMER, CustomerVariables.CITY)

    val customerData = cusTop5.join(salesOrderAddress, cusTop5(CustomerVariables.FK_CUSTOMER) === salesOrderAddress(SalesOrderVariables.FK_CUSTOMER), SQL.INNER)
      .select(
        cusTop5(CustomerVariables.FK_CUSTOMER),
        cusTop5(ProductVariables.BRAND),
        salesOrderAddress(CustomerVariables.CITY)
      )

    customerData
  }

}
