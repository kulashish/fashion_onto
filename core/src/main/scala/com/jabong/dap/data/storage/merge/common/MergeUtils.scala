package com.jabong.dap.data.storage.merge.common

import com.jabong.dap.common.{ ArrayUtils, Spark }
import org.apache.spark.sql.{ DataFrame, _ }

/**
 * Merges the dataFrames and returns the merged dataFrame.
 */

object MergeUtils extends MergeData {

  val NEW_ = "new_"

  def InsertUpdateMerge(dfBase: DataFrame, dfIncr: DataFrame, primaryKey: String): DataFrame = {

    // join on primary key
    val joinedDF = joinOldAndNewDF(dfBase: DataFrame, dfIncr: DataFrame, primaryKey: String)

    val dfSchema = dfIncr.schema

    val numOfColumns = dfSchema.length

    val incrPKColumn = ArrayUtils.findIndexInArray(dfIncr.columns, primaryKey)

    def reduceFunc(x: Row): Row = {
      val splitSeq = x.toSeq.splitAt(numOfColumns)
      if (x(incrPKColumn + numOfColumns) == null)
        Row.fromSeq(splitSeq._1)
      else
        Row.fromSeq(splitSeq._2)
    }

    val mergedDF = joinedDF.map(x => reduceFunc(x))

    Spark.getSqlContext().createDataFrame(mergedDF, dfSchema)
  }

  // join old and new data frame
  def joinOldAndNewDF(dfIncr: DataFrame, dfPrevVarFull: DataFrame, primaryKey: String): DataFrame = {

    var dfIncrVar = dfIncr

    dfIncrVar = Spark.getContext().broadcast(dfIncrVar).value

    val dfSchema = dfIncr.schema

    // rename dfIncr column names with new_ as prefix
    dfSchema.foreach(x => dfIncrVar = dfIncrVar.withColumnRenamed(x.name, NEW_ + x.name))

    // join old and new data frame on primary key
    val joinedDF = dfPrevVarFull.join(dfIncrVar, dfPrevVarFull(primaryKey) === dfIncrVar(NEW_ + primaryKey), "outer")

    joinedDF
  }

}
