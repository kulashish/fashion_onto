package com.jabong.dap.model.utils

import java.io.File

import com.jabong.dap.common.Spark
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Created by raghu on 8/7/15.
 */
object Utils {

  val NEW_ = "new_"

  // join old and new data frame
  def joinOldAndNewDF(dfIncr: DataFrame, primaryKey: String, dataSets: String, date: String): DataFrame = {

    val oldDateFolder = File.separator + date.replaceAll("-", File.separator) + File.separator

    val dfFull = Spark.getSqlContext().read.parquet(DataSets.VARIABLE_PATH + dataSets + "/full" + oldDateFolder)

    var dfIncrVar = Spark.getContext().broadcast(dfIncr).value

    val dfSchema = dfIncr.schema

    // rename dfIncr column names with new_ as prefix
    dfSchema.foreach(x => dfIncrVar = dfIncrVar.withColumnRenamed(x.name, NEW_ + x.name))

    // join old and new data frame on primary key
    val joinedDF = dfFull.join(dfIncrVar, dfFull(primaryKey) === dfIncrVar(NEW_ + primaryKey), "outer")

    joinedDF
  }

}
