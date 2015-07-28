package com.jabong.dap.common.json

import java.io.File

import com.jabong.dap.common.Spark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

/**
 * Created by raghu on 25/6/15.
 */
object JsonUtils {

  val TEST_RESOURCES = "src" + File.separator + "test" + File.separator + "resources"

  //Reads Parquet file, convert to dataframe, writes it in Json format file for test cases
  def writeToJson(parquetFilePath: String, fileName: String): Any = {

    val df = Spark.getSqlContext().read.parquet(parquetFilePath + fileName + File.separator)

    df.limit(5).select("*").write.format("json").json(TEST_RESOURCES + File.separator + fileName + ".json")

  }

  //Reads Parquet file, convert to dataframe, writes it in Json format file for test cases
  def writeToJson(parquetFilePath: String, fileName: String, filterCond: String): Any = {

    val df = Spark.getSqlContext().read.parquet(parquetFilePath + fileName + File.separator)

    df.filter(filterCond).select("*").write.format("json").json(TEST_RESOURCES + File.separator + fileName + ".json")

  }

  //read Json file
  def readFromJson(directoryName: String, fileName: String, schema: StructType): DataFrame = {
    val df = Spark.getSqlContext().read.schema(schema).format("json")
      .load(TEST_RESOURCES + File.separator + directoryName + File.separator + fileName + ".json")

    df
  }

  def readFromJson(directoryName: String, fileName: String): DataFrame = {
    val df = Spark.getSqlContext().read.format("json")
      .load(TEST_RESOURCES + File.separator + directoryName + File.separator + fileName + ".json")

    df
  }

}
