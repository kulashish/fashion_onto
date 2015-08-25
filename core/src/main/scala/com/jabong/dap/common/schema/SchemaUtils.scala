package com.jabong.dap.common.schema

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ DataType, StructType }

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

  private def addColumn(df: DataFrame, key: String, dataType: DataType): DataFrame = {
    //TODO Add check for datatype as well.
    if (df.columns.contains(key)) {
      return df
    } else {
      return df.withColumn(key, lit(null).cast(dataType))
    }
  }

  /**
   *
   * @param df
   * @param schema
   * @return
   */
  def changeSchema(df: DataFrame, schema: StructType): DataFrame = {
    var res: DataFrame = df
    schema.foreach(e => (res = addColumn(res, e.name, e.dataType)))
    return res

  }
}
