package com.jabong.dap.data.storage.merge.common

import com.jabong.dap.common.Spark
import org.apache.spark.sql.DataFrame

/**
 * Merges the dataFrames and returns the merged dataFrame.
 */

object MergeUtils extends MergeData {

  val NEW_ = "new_"

  def InsertUpdateMerge(dfBase: DataFrame, dfIncr: DataFrame, primaryKey: String): DataFrame = {
    val newpk = NEW_ + primaryKey
    if (null == dfBase)
      return dfIncr
    else if (null == dfIncr)
      return dfBase

    // join on primary key
    val joinedDF = joinOldAndNewDF(dfIncr, dfBase, primaryKey)

    //    //Commenting this code as this has functionality issue
    //    //when we have a data set with base as big and incr as very small.
    //    val dfSchema = dfIncr.schema
    //
    //    val numOfColumns = dfSchema.length
    //
    //    val incrPKColumn = ArrayUtils.findIndexInArray(dfIncr.columns, primaryKey)
    //    
    //    def reduceFunc(x: Row): Row = {
    //      val splitSeq = x.toSeq.splitAt(numOfColumns)
    //      if (x(incrPrimayKeyColumn + numOfColumns) == null)
    //        Row.fromSeq(splitSeq._1)
    //      else
    //        Row.fromSeq(splitSeq._2)
    //    }
    //
    //    val mergedDF = joinedDF.map(x => reduceFunc(x))
    //
    //    Spark.getSqlContext().createDataFrame(mergedDF, dfSchema)

    var numPart = dfBase.rdd.partitions.length

    val df1 = joinedDF.filter(newpk + " IS NULL").select(dfBase("*"))

    df1.unionAll(dfIncr).coalesce(numPart)
  }

  /**
   * join old and new data frame
   * @param dfIncr
   * @param dfPrevVarFull
   * @param primaryKey
   * @return DataFrame
   */
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
