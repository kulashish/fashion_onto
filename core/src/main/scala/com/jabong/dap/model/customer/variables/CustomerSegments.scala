package com.jabong.dap.model.customer.variables

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.variables.CustomerSegmentsVariables
import org.apache.spark.sql.types.{ IntegerType, StringType, StructField, StructType }
import org.apache.spark.sql.{ DataFrame, Row }

/**
 * Created by raghu on 25/6/15.
 */
object CustomerSegments {

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // DataFrame CustomerSegments operations
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
