package com.jabong.dap.common.json

import com.jabong.dap.common.Spark
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Created by raghu on 25/6/15.
 */
object JsonUtils {

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
