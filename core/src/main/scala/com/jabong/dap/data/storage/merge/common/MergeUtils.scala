package com.jabong.dap.data.storage.merge.common

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.schema.SchemaUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ Column, DataFrame, Row }
/**
 * Merges the dataFrames and returns the merged dataFrame.
 */

object MergeUtils extends MergeData {

  val NEW_ = "new_"

  def InsertUpdateMerge(dfBase: DataFrame, dfIncr: DataFrame, primaryKey: String): DataFrame = {
    if (null == dfBase)
      return dfIncr
    else if (null == dfIncr)
      return dfBase
    else if (0 == dfIncr.count())
      return dfBase

    var dfBaseNew = dfBase
    if (!SchemaUtils.isSchemaEqual(dfIncr.schema, dfBase.schema)) {
      dfBaseNew = SchemaUtils.changeSchema(dfBase, dfIncr.schema)
    }

    // join on primary key
    val joinedDF = joinOldAndNewDF(dfIncr, dfBaseNew, primaryKey)

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

    var numPart = dfBaseNew.rdd.partitions.length

    val df1 = joinedDF.filter(dfIncr(primaryKey).isNull).select(dfBaseNew("*"))

    df1.unionAll(dfIncr).dropDuplicates().coalesce(numPart)
  }

  /**
   * joinOldAndNewDF with null parameters allowed
   * @param dfIncr can be null
   * @param incrSchema schema
   * @param dfPrevVarFull can be null
   * @param prevVarFullSchema schema
   * @param primaryKey join key
   * @return Dataframe
   */
  def joinOldAndNewDF(dfIncr: DataFrame, incrSchema: StructType, dfPrevVarFull: DataFrame, prevVarFullSchema: StructType, primaryKey: String): DataFrame = {
    var dfIncrVar: DataFrame = dfIncr
    if (null == dfIncr) dfIncrVar = Spark.getSqlContext().createDataFrame(Spark.getContext().emptyRDD[Row], incrSchema)

    var dfPrevVarFullVar: DataFrame = dfPrevVarFull
    if (null == dfPrevVarFull) dfPrevVarFullVar = Spark.getSqlContext().createDataFrame(Spark.getContext().emptyRDD[Row], prevVarFullSchema)

    joinOldAndNewDF(dfIncrVar, dfPrevVarFullVar, primaryKey)
  }

  /**
   * This join is null safe (<=>).
   * @param oldDF can be null
   * @param newDF can be null
   * @param keys tuple2 of keys pair
   * @param joinType default is given for unambiguity
   * @return return the result with newDF schema renamed with prefix "new_" to each field name
   */
  def joinOldAndNew(newDF: DataFrame, newSchema: StructType, oldDF: DataFrame, oldSchema: StructType, keys: List[(String, String)], joinType: String): DataFrame = {
    if (keys.length < 1) return null

    val oldNullSafe = if (null == oldDF) Spark.getSqlContext().createDataFrame(Spark.getContext().emptyRDD[Row], oldSchema) else oldDF
    val newNullSafe = if (null == newDF) Spark.getSqlContext().createDataFrame(Spark.getContext().emptyRDD[Row], newSchema) else newDF

    val newDFNullSafeRenamed = SchemaUtils.renameCols(newNullSafe, NEW_)
    val joinExpr: Column = keys.slice(1, keys.size).foldLeft(oldNullSafe(keys(0)._1) <=> newDFNullSafeRenamed(NEW_ + keys(0)._2))((m: Column, n: (String, String)) => m && oldNullSafe(n._1) <=> newDFNullSafeRenamed(NEW_ + n._2))
    oldNullSafe.join(newDFNullSafeRenamed, joinExpr, joinType)
  }

  /**
   * join old and new data frame
   * @param dfIncr
   * @param dfPrevVarFull
   * @param primaryKey
   * @return DataFrame
   */
  private def joinOldAndNewDF(dfIncr: DataFrame, dfPrevVarFull: DataFrame, primaryKey: String): DataFrame = {

    var dfIncrVar = dfIncr.dropDuplicates()

    dfIncrVar = Spark.getContext().broadcast(dfIncrVar).value

    val dfSchema = dfIncr.schema

    // join old and new data frame on primary key
    val joinedDF = dfPrevVarFull.dropDuplicates().join(dfIncrVar, dfPrevVarFull(primaryKey) === dfIncrVar(primaryKey), SQL.FULL_OUTER)

    joinedDF
  }
}
