package com.jabong.dap.common.schema

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

/**
 * Created by raghu on 3/7/15.
 */
object SchemaUtils {
  //checks if two schemas are Equal
  def isSchemaEqual(schemaFirst: StructType, schemaSecond: StructType): Boolean = {
    val fieldTypesFirst = schemaFirst.map(field => s"${field.name}:${field.dataType.simpleString}").toSet

    val fieldTypesSecond = schemaSecond.map(field => s"${field.name}:${field.dataType.simpleString}").toSet

    if (!fieldTypesFirst.equals(fieldTypesSecond)) {

      log("schema attributes or data type mismatch, it should be: " + schemaSecond)

      return false
    }
    return true
  }
}
