package com.jabong.dap.common.json

import java.io.File

import com.jabong.dap.common.Spark
import com.jabong.dap.data.storage.merge.common.DataVerifier
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import scala.collection.mutable.ListBuffer

/**
 * Created by raghu on 25/6/15.
 */
object JsonUtils {

  val TEST_RESOURCES = "src" + File.separator + "test" + File.separator + "resources"

  //Reads Parquet file, convert to dataframe, writes it in Json format file for test cases
  def writeToJson(parquetFilePath: String, fileName: String, jsonFilePath: String): Any = {

    val df = Spark.getSqlContext().read.parquet(parquetFilePath + File.separator + fileName + File.separator)

    val srcFile = TEST_RESOURCES + File.separator + fileName + ".json"
    df.select("*").coalesce(1).write.format("json").json(srcFile)

    val destFile = jsonFilePath + File.separator + fileName + ".json"
    DataVerifier.rename(srcFile + "/part-00000", destFile)

  }

  //Reads Parquet file, convert to dataframe, writes it in Json format file for test cases
  def writeToJson(parquetFilePath: String, fileName: String, jsonFilePath: String, filterCond: String): Any = {

    val df = Spark.getSqlContext().read.parquet(parquetFilePath + File.separator + fileName + File.separator)

    val srcFile = TEST_RESOURCES + File.separator + fileName + ".json"
    df.filter(filterCond).select("*").coalesce(1).write.format("json").json(srcFile)

    val destFile = jsonFilePath + File.separator + fileName + ".json"
    DataVerifier.rename(srcFile + "/part-00000", destFile)
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

  def jsonsFile2ArrayOfMap(directoryName: String, fileName: String): List[Map[String,String]] ={
      val TEST_RESOURCES = "src" + File.separator + "test" + File.separator + "resources"
      val file = TEST_RESOURCES + File.separator + directoryName + File.separator + fileName + ".json"
      val lines = scala.io.Source.fromFile(file).mkString
      val arrayJson = lines.split("\n")
      var listMaps: ListBuffer[Map[String, String]] = ListBuffer()
      for(json <- arrayJson)  {
        val map =json.trim.substring(1, json.length - 1)
          .split(",")
          .map(_.split(":"))
          .map { case Array(k, v) => (k.dropRight(1).drop(1), v filterNot ("\"" contains _))}
          .toMap
        listMaps += map
      }
    return listMaps.toList
  }
}
