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
    true
  }

  private def addColumn(df: DataFrame, key: String, dataType: DataType): DataFrame = {
    if (df.columns.contains(key)) {
      df
    } else {
      df.withColumn(key, lit(null).cast(dataType))
    }
  }

  def dropColumns(df: DataFrame, schema: StructType): DataFrame = {
    var res: DataFrame = df
    var sec = df.schema
    sec.foreach(e => (res = dropColumn(res, e.name, schema)))
    res
  }

  private def dropColumn(df: DataFrame, key: String, schema: StructType): DataFrame = {
    if (schema.fieldNames.contains(key)) {
      df
    } else {
      df.drop(key)
    }
  }

  /**
   *
   * @param df
   * @param schema
   * @return
   */
  def addColumns(df: DataFrame, schema: StructType): DataFrame = {
    var res: DataFrame = df
    schema.foreach(e => (res = addColumn(res, e.name, e.dataType)))
    res

  }

  /**
   *
   * @param df
   * @param schema
   * @return
   */
  def changeSchema(df: DataFrame, schema: StructType): DataFrame = {
    //TODO Check for datatypes changes.
    var res: DataFrame = df
    // Adding any new columns to DF with null value.
    schema.foreach(e => (res = addColumn(res, e.name, e.dataType)))
    // Deleting any extra columns in DF.
    var sec = df.schema
    sec.foreach(e => (res = dropColumn(res, e.name, schema)))
    res
  }

  /**
   * rename every column with prefix given
   * @param df
   * @param prefix
   * @return
   */
  def renameCols(df: DataFrame, prefix: String): DataFrame = {
    var dfVar = df
    val schema = df.schema
    schema.foreach(x => dfVar = dfVar.withColumnRenamed(x.name, prefix + x.name))
    dfVar
  }
}
