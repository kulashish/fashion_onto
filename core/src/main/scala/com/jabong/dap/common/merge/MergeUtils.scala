package com.jabong.dap.common.merge

import java.io.File

import com.jabong.dap.common.{ ArrayUtils, Spark }
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.{ DataFrame, _ }

object MergeUtils extends MergeData  {

  val NEW_ = "new_"

  def InsertUpdateMerge(dfBase: DataFrame, dfIncr: DataFrame, primaryKey: String): DataFrame = {
    // rename dfIncr column names with new_ as prefix
    var dfIncrVar = dfIncr;

    val dfSchema = dfIncr.schema
    val numOfColumns = dfSchema.length
    val incrPrimayKeyColumn = ArrayUtils.findIndexInArray(dfIncr.columns, primaryKey)

    dfSchema.foreach(x => dfIncrVar = dfIncrVar.withColumnRenamed(x.name, "new_" + x.name))
    // join on primary key
    val joinedDF = dfBase.join(dfIncrVar, dfBase(primaryKey) === dfIncrVar("new_" + primaryKey), "outer")

    def reduceFunc(x: Row): Row = {
      val splitSeq = x.toSeq.splitAt(numOfColumns)
      if (x(incrPrimayKeyColumn + numOfColumns) == null)
        Row.fromSeq(splitSeq._1)
      else
        Row.fromSeq(splitSeq._2)
    }

    val mergedDF = joinedDF.map(x => reduceFunc(x))

    Spark.getSqlContext().createDataFrame(mergedDF, dfSchema)
  }

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

