package com.jabong.dap.common

import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 25/6/15.
 */
object Utils {

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

  //Reads Parquet file, convert to dataframe, writes it in Json format file for test cases
  def writeToJson(fileName: String, parquetFilePath: String): Any = {

    val df = Spark.getSqlContext().read.parquet(parquetFilePath + fileName + "/")

    df.limit(5).select("*").write.format("json").json(DataSets.TEST_RESOURCES + fileName + ".json")

  }

  //read Json file
  def readFromJson(directoryName: String, fileName: String, schema: StructType): DataFrame = {

    val df = Spark.getSqlContext().read.schema(schema).format("json")
      .load(DataSets.TEST_RESOURCES + directoryName + "/" + fileName + ".json")

    df
  }

}
