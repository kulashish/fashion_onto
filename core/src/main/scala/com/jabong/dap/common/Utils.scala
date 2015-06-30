package com.jabong.dap.common

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 25/6/15.
 */
object Utils {

  //check two schema is Equals
  def isSchemaEquals(schemaFirst: StructType, schemaSecond: StructType): Boolean ={

    val fieldTypesFirst = schemaFirst.map(field => s"${field.name}:${field.dataType.simpleString}").toSet

    val fieldTypesSecond = schemaSecond.map(field => s"${field.name}:${field.dataType.simpleString}").toSet

    if(!fieldTypesFirst.equals(fieldTypesSecond)){

      log("schema attributes or data type mismatch, it should be: " + schemaSecond)

      return false
    }

    return true
  }

//write file Parquet to Json
  def writeToJson(fileName: String): Any={

    //change this path according to your file path
    var BOB_PATH =  "/home/raghu/bigData/parquetFiles/"

    val df = Spark.getSqlContext().read.parquet(BOB_PATH + fileName + "/")

    df.limit(5).select("*").write.format("json").json(DataFiles.TEST_RESOURCES + fileName + ".json")

  }

  //read Json file
  def readFromJson(directoryName:String, fileName: String, schema: StructType): DataFrame = {

    val df = Spark.getSqlContext().read.schema(schema).format("json")
      .load(DataFiles.TEST_RESOURCES + directoryName + "/" + fileName + ".json")

    df
  }

}
