package com.jabong.dap.campaign.skuselection

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.variables.{ ProductVariables, CustomerVariables }
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by rahul on 18/11/15.
 */
object MostBought extends Logging {

  /**
   *
   * @param customerData
   * @param itrDataFrame
   * @return
   */
  def skuFilter(customerTopData: DataFrame, customerData: DataFrame, itrDataFrame: DataFrame, topFieldList: String): DataFrame = {
    if (customerData == null || itrDataFrame == null) {
      return null
    }

    val topField = topFieldList.substring(0, topFieldList.indexOf("_"))
    val customerTopBrandSku = CampaignUtils.getFavSku(customerTopData, CustomerVariables.FK_CUSTOMER, topFieldList).
      select(col(CustomerVariables.FK_CUSTOMER),
        col(topFieldList) as topField,
        col(ProductVariables.SKU_SIMPLE))

    CampaignUtils.debug(customerTopBrandSku, CampaignCommon.LOVE_CALENDAR_CAMPAIGNS+"customerTopBrandSku after filteredSku ")


    val customerTopDataWithItr = customerData.join(customerTopBrandSku, customerData(CustomerVariables.FK_CUSTOMER) ===
      customerTopBrandSku(CustomerVariables.FK_CUSTOMER), SQL.INNER).join(itrDataFrame,
      itrDataFrame(ProductVariables.SKU_SIMPLE) === customerTopBrandSku(ProductVariables.SKU_SIMPLE)).
      select(
        customerData(CustomerVariables.FK_CUSTOMER),
        customerData(CustomerVariables.EMAIL),
        itrDataFrame(ProductVariables.SKU_SIMPLE),
        itrDataFrame(ProductVariables.SPECIAL_PRICE),
        itrDataFrame(ProductVariables.BRICK),
        itrDataFrame(ProductVariables.BRAND),
        itrDataFrame(ProductVariables.MVP),
        itrDataFrame(ProductVariables.GENDER),
        itrDataFrame(ProductVariables.PRODUCT_NAME),
        itrDataFrame(ProductVariables.STOCK),
        itrDataFrame(ProductVariables.COLOR),
        itrDataFrame(ProductVariables.PRICE_BAND))

    CampaignUtils.debug(customerTopBrandSku, CampaignCommon.LOVE_CALENDAR_CAMPAIGNS+"customerTopBrandSku inside Most Bought ")

    return customerTopDataWithItr
  }
}
