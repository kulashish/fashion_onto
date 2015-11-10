package com.jabong.dap.campaign.skuselection

import com.jabong.dap.campaign.skuselection.NewArrivalsBrand._
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.variables.{ItrVariables, CustomerVariables, SalesCartVariables, ProductVariables}
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by samathashetty on 9/11/15.
 */
object HottestXFilter extends Logging{
  def skuFilter(customerSelected: DataFrame, itrData: DataFrame): DataFrame = {

    if (customerSelected == null || itrData == null) {

      logger.error("Data frame should not be null")

      return null

    }

    val dfResult = customerSelected.join(itrData, customerSelected(SalesCartVariables.SKU) === itrData(ProductVariables.SKU_SIMPLE), SQL.INNER)
      .select(
        col(SalesCartVariables.FK_CUSTOMER),
        col(SalesCartVariables.EMAIL),
        col(SalesCartVariables.SKU) as ProductVariables.SKU_SIMPLE,
        col(ProductVariables.SPECIAL_PRICE),
        col(ProductVariables.BRICK),
        col(ProductVariables.GENDER),
        col(ProductVariables.MVP),
        col(ProductVariables.PRODUCT_NAME)
      )

    return dfResult
  }


}
