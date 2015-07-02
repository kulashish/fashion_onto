package com.jabong.dap.model.customer.variables

import com.jabong.dap.common.{ Spark, Utils }
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.{ Row, DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ StringType, IntegerType, StructField, StructType }

/**
 * Created by raghu on 25/6/15.
 */
object CustomerSegments {

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //customer_segments variable schemas
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  val mvp_seg = StructType(Array(StructField("fk_customer", IntegerType, true),
    StructField("mvp_score", IntegerType, true),
    StructField("segment", IntegerType, true)))

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // DataFrame CustomerSegments operations
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  //Name of variable: fk_customer, MVP, Segment0, Segment1,Segment2, Segment3, Segment4, Segment5, Segment6
  //calculate mvp_score and  latest updated value of mvp_score from customer_segments
  def getMvpAndSeg(dfCustomerSegments: DataFrame): DataFrame = {

    if (dfCustomerSegments == null) {

      log("Data frame should not be null")

      return null

    }

    if (!Utils.isSchemaEqual(dfCustomerSegments.schema, Schema.customerSegments)) {

      log("schema attributes or data type mismatch")

      return null

    }

    val dfCustSegVars = dfCustomerSegments.select("fk_customer", "updated_at", "mvp_score", "segment")
      .sort(col("fk_customer"), desc("updated_at"))
      .groupBy("fk_customer")
      .agg(first("mvp_score") as "mvp_score",
        first("segment") as "segment")

    //    val segments = getSeg(dfCustSegVars)

    dfCustSegVars
  }

  def getSeg(dfCustSegVars: DataFrame): DataFrame = {

    val schema = StructType(Array(StructField("fk_customer", IntegerType, true),
      StructField("mvp_score", IntegerType, true),
      StructField("segment0", StringType, true),
      StructField("segment1", StringType, true),
      StructField("segment2", StringType, true),
      StructField("segment3", StringType, true),
      StructField("segment4", StringType, true),
      StructField("segment5", StringType, true),
      StructField("segment6", StringType, true)))

    val segments = dfCustSegVars.map(r => r(0) + "," + r(1) + "," + getSegValue(r(2).toString))

    // Convert records of the RDD (segments) to Rows.
    val rowRDD = segments.map(_.split(","))
      .map(r => Row(r(0).trim,
        r(1).trim,
        r(2).trim,
        r(3).trim,
        r(4).trim,
        r(5).trim,
        r(6).trim,
        r(7).trim,
        r(8).trim))

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
