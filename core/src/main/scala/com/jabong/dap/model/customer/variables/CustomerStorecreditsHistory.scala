package com.jabong.dap.model.customer.variables

import com.jabong.dap.common.Utils
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ TimestampType, IntegerType, StructField, StructType }

/**
 * Created by raghu on 25/6/15.
 */
object CustomerStorecreditsHistory {

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //customer_storecredits_history variable schemas
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  val last_jr_covert_date = StructType(Array(
    StructField("fk_customer", IntegerType, true),
    StructField("last_jr_covert_date", TimestampType, true)
  ))

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

    if (!Utils.isSchemaEquals(dfCSH.schema, Schema.csh)) {

      log("schema attributes or data type mismatch")

      return null

    }

    val dfLastJrCovertDate = dfCSH.select("fk_customer", "created_at")
      .groupBy("fk_customer")
      .agg(max("created_at") as "last_jr_covert_date")

    dfLastJrCovertDate
  }

}
