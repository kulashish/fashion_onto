package com.jabong.dap.model.clickstream.schema

import com.jabong.dap.model.clickstream.variables.ClickstreamFields
import org.apache.hadoop.hdfs.util.Diff.ListType
import org.apache.spark.sql.types._

object PagevisitSchema {

  val userAttribute = StructType(Array(
    StructField(ClickstreamFields.USER_ID, StringType, true),
    StructField(ClickstreamFields.PAGETS, TimestampType, true),
    StructField(ClickstreamFields.BROWSER_ID, StringType, true),
    StructField(ClickstreamFields.DEVICE, StringType, true),
    StructField(ClickstreamFields.DOMAIN, StringType, true),
    StructField(ClickstreamFields.PAGETYPE, StringType, true),
    StructField(ClickstreamFields.ACTUAL_VISITID, StringType, true),
    StructField(ClickstreamFields.VISITTS, StringType, true),
    StructField(ClickstreamFields.PRODUCT_SKU, StringType, true),
    StructField(ClickstreamFields.BRAND, StringType, true)
  ))

  val incremental = StructType(Array(
    StructField("userid", StringType, true),
    StructField("dt", StringType, true),
    StructField("dt", ArrayType(StringType,true), true)
  ))
}