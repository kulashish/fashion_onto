package com.jabong.dap.data.storage.merge.common

import com.jabong.dap.common.ArrayUtils
import org.apache.spark.sql.DataFrame

/**
 * Merges the dataFrames and returns the merged dataFrame.
 */

object MergeUtils extends MergeData {

  def InsertUpdateMerge(dfBase: DataFrame, dfIncr: DataFrame, primaryKey: String): DataFrame = {
    // rename dfIncr column names with new_ as prefix
    var dfIncrVar = dfIncr

    val dfSchema = dfIncr.schema
    val numOfColumns = dfSchema.length
    val incrPrimayKeyColumn = ArrayUtils.findIndexInArray(dfIncr.columns, primaryKey)

    dfSchema.foreach(x => dfIncrVar = dfIncrVar.withColumnRenamed(x.name, "new_" + x.name))

    val newpk = "new_" + primaryKey
    // join on primary key
    val joinedDF = dfBase.join(dfIncrVar, dfBase(primaryKey) === dfIncrVar(newpk), "outer")

//    //Commenting this code as this has functionality issue
//    //when we have a data set with base as big and incr as very small.
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
}
