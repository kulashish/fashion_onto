package com.jabong.dap.model.customer.variables

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.variables.CustomerSegmentsVariables
import com.jabong.dap.common.schema.SchemaUtils
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

/**
 * Created by raghu on 25/6/15.
 */
object CustomerSegments {

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // DataFrame CustomerSegments operations
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  //Name of variable: fk_customer, MVP, Segment0, Segment1,Segment2, Segment3, Segment4, Segment5, Segment6
  //calculate mvp_score and  latest updated value of mvp_score from customer_segments
  def getCustomerSegments(dfCustomerSegments: DataFrame): DataFrame = {

    if (dfCustomerSegments == null) {

      log("Data frame should not be null")

      return null

    }

    if (!SchemaUtils.isSchemaEqual(dfCustomerSegments.schema, Schema.customerSegments)) {

      log("schema attributes or data type mismatch")

      return null

    }

    val dfInc = dfCustomerSegments.select(
      CustomerSegmentsVariables.FK_CUSTOMER,
      CustomerSegmentsVariables.UPDATED_AT,
      CustomerSegmentsVariables.MVP_SCORE,
      CustomerSegmentsVariables.SEGMENT,
      CustomerSegmentsVariables.DISCOUNT_SCORE
      )
      .sort(desc(CustomerSegmentsVariables.UPDATED_AT))
      .groupBy(CustomerSegmentsVariables.FK_CUSTOMER)
      .agg(
        first(CustomerSegmentsVariables.MVP_SCORE) as CustomerSegmentsVariables.MVP_TYPE,
        first(CustomerSegmentsVariables.SEGMENT),
        first(CustomerSegmentsVariables.DISCOUNT_SCORE)
      )

    //    val segments = getSeg(dfCustSegVars)

    dfInc
  }

  def getSeg(dfCustSegVars: DataFrame): DataFrame = {

    val schema = StructType(Array(
      StructField(CustomerSegmentsVariables.FK_CUSTOMER, IntegerType, true),
      StructField(CustomerSegmentsVariables.MVP_SCORE, IntegerType, true),
      StructField(CustomerSegmentsVariables.SEGMENT0, StringType, true),
      StructField(CustomerSegmentsVariables.SEGMENT1, StringType, true),
      StructField(CustomerSegmentsVariables.SEGMENT2, StringType, true),
      StructField(CustomerSegmentsVariables.SEGMENT3, StringType, true),
      StructField(CustomerSegmentsVariables.SEGMENT4, StringType, true),
      StructField(CustomerSegmentsVariables.SEGMENT5, StringType, true),
      StructField(CustomerSegmentsVariables.SEGMENT6, StringType, true)
    ))

    val segments = dfCustSegVars.map(r => r(0) + "," + r(1) + "," + getSegValue(r(2).toString))

    // Convert records of the RDD (segments) to Rows.
    val rowRDD = segments.map(_.split(","))
      .map(r => Row(
        r(0).trim,
        r(1).trim,
        r(2).trim,
        r(3).trim,
        r(4).trim,
        r(5).trim,
        r(6).trim,
        r(7).trim,
        r(8).trim
      ))

    // Apply the schema to the RDD.
    val dfs = Spark.getSqlContext().createDataFrame(rowRDD, schema)

    dfs
  }

  def getSegValue(i: String): String = {
    val x = Integer.parseInt(i)
    x match {
      case 0 => return "YES,NO,NO,NO,NO,NO,NO"
      case 1 => return "NO,YES,NO,NO,NO,NO,NO"
      case 2 => return "NO,NO,YES,NO,NO,NO,NO"
      case 3 => return "NO,NO,NO,YES,NO,NO,NO"
      case 4 => return "NO,NO,NO,NO,YES,NO,NO"
      case 5 => return "NO,NO,NO,NO,NO,YES,NO"
      case 6 => return "NO,NO,NO,NO,NO,NO,YES"
    }
    "NO,NO,NO,NO,NO,NO,NO"
  }

}
