package com.jabong.dap.common.schema

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructType}

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

  def addColumn(df: DataFrame, key: String, dataType: DataType):DataFrame={
    if(df.columns.contains(key)){
      return df
    } else {
      var i: Int = 0
      var f: Int = 0
      df.schema.iterator.foreach(e => (
        if(e.dataType == dataType){
          f=i
        }else{
          i = i+1
        }))
      df.withColumn(key,df(df.columns(f)).leq(null))
    }
  }

  /**
   *
   * @param df
   * @param schema
   * @return
   */
  def changeSchema(df: DataFrame, schema: StructType): DataFrame={
    var res: DataFrame =df
    schema.foreach(e =>(res = addColumn(res, e.name, e.dataType)))
    ///Now only for specific schema with 6 cols for campaign data need to change this
    return res.select(
    res(schema(0).name),
    res(schema(1).name),
    res(schema(2).name),
    res(schema(3).name),
    res(schema(4).name),
    res(schema(5).name))
  }
}
