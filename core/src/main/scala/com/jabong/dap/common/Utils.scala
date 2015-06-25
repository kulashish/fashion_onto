package com.jabong.dap.common

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 25/6/15.
 */
object Utils {
  //check two schema is Equals
  def isEquals(schemaFirst: StructType, schemaSecond: StructType): Boolean ={

    val fieldTypesFirst = schemaFirst.map(field => s"${field.name}:${field.dataType.simpleString}").toSet
    val fieldTypesSecond = schemaSecond.map(field => s"${field.name}:${field.dataType.simpleString}").toSet

    if(!fieldTypesFirst.equals(fieldTypesSecond)){

      log("schema attributes or data type mismatch, it should be: " + schemaSecond)

      return false
    }

    return true
  }
}
