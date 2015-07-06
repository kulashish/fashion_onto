package com.jabong.dap.model.customer.variables

import com.jabong.dap.common.constants.variables.CustomerStoreVariables
import com.jabong.dap.common.schema.SchemaUtils
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 25/6/15.
 */
object CustomerStorecreditsHistory {

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // DataFrame CUSTOMER_STORECREDITS_HISTORY operations
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  //Name of variable: fk_customer, LAST_JR_COVERT_DATE
  //customer_storecredits_history.operation_type = "nextbee_points_added", latest date for fk_customer
  def getLastJrCovertDate(dfCSH: DataFrame): DataFrame = {

    if (dfCSH == null) {

      log("Data frame should not be null")

      return null

    }

    if (!SchemaUtils.isSchemaEqual(dfCSH.schema, Schema.csh)) {

      log("schema attributes or data type mismatch")

      return null

    }

    val dfLastJrCovertDate = dfCSH.select(CustomerStoreVariables.FK_CUSTOMER, CustomerStoreVariables.CREATED_AT)
      .groupBy(CustomerStoreVariables.FK_CUSTOMER)
      .agg(max(CustomerStoreVariables.CREATED_AT) as CustomerStoreVariables.LAST_JR_COVERT_DATE)

    dfLastJrCovertDate
  }

}
