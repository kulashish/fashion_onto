package com.jabong.dap.common.json

import java.io.File

import com.jabong.dap.common.Spark
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

/**
 * Created by raghu on 25/6/15.
 */
object JsonUtils {

  //Reads Parquet file, convert to dataframe, writes it in Json format file for test cases
  def writeToJson(parquetFilePath: String, fileName: String): Any = {

    val df = Spark.getSqlContext().read.parquet(parquetFilePath + fileName + File.separator)

    df.limit(5).select("*").write.format("json").json(DataSets.TEST_RESOURCES + File.separator + fileName + ".json")

  }

  //read Json file
  def readFromJson(directoryName: String, fileName: String, schema: StructType): DataFrame = {
    val df = Spark.getSqlContext().read.schema(schema).format("json")
      .load(DataSets.TEST_RESOURCES + File.separator + directoryName + File.separator + fileName + ".json")

    df
  }

}
