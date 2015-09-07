package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.constants.variables.SalesCartVariables
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 7/9/15.
 */
class NewArrivalsBrand extends CustomerSelector with Logging {

  override def customerSelection(salesCart30Day: DataFrame): DataFrame = {
    if (salesCart30Day == null) {

      logger.error("Data frame should not be null")

      return null

    }

    val dfCustomerSelection = salesCart30Day.filter(SalesCartVariables.STATUS + " = '" + SalesCartVariables.ACTIVE + "' or " + SalesCartVariables.STATUS + " = '" + SalesCartVariables.PURCHASED + "'")
      .select(SalesCartVariables.FK_CUSTOMER, SalesCartVariables.EMAIL, SalesCartVariables.SKU)

    return dfCustomerSelection
  }

  override def customerSelection(inData: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, inData3: DataFrame): DataFrame = ???

}
