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

    val df1 = joinedDF.filter(joinedDF(NEW_ + primaryKey).isNull).select(dfBaseNew("*"))

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

  def joinOldAndNewDF(dfIncr: DataFrame, incrSchema: StructType, dfPrevVarFull: DataFrame, prevVarFullSchema: StructType, primaryKey1: String, primaryKey2: String): DataFrame = {
    var dfIncrVar: DataFrame = dfIncr
    if (null == dfIncr) dfIncrVar = Spark.getSqlContext().createDataFrame(Spark.getContext().emptyRDD[Row], incrSchema)

    var dfPrevVarFullVar: DataFrame = dfPrevVarFull
    if (null == dfPrevVarFull) dfPrevVarFullVar = Spark.getSqlContext().createDataFrame(Spark.getContext().emptyRDD[Row], prevVarFullSchema)

    joinOldAndNewDF(dfIncrVar, dfPrevVarFullVar, primaryKey1, primaryKey2)
  }

  def joinOldAndNewDF(dfIncr: DataFrame, dfPrevVarFull: DataFrame, primaryKey1: String, primaryKey2: String): DataFrame = {

    var dfIncrVar = dfIncr.dropDuplicates()

    dfIncrVar = Spark.getContext().broadcast(dfIncrVar).value

    dfIncrVar = SchemaUtils.renameCols(dfIncrVar, NEW_)

    // join old and new data frame on primary key
    val joinedDF = dfPrevVarFull.dropDuplicates().join(dfIncrVar, dfPrevVarFull(primaryKey1) === dfIncrVar(NEW_ + primaryKey1) && dfPrevVarFull(primaryKey2) === dfIncrVar(NEW_ + primaryKey2), SQL.FULL_OUTER)

    joinedDF
  }

  /**
   * @param oldDF can be null
   * @param newDF can be null
   * @param keys tuple2 of keys pair
   * @param joinType default is given for unambiguity
   * @return return the result with newDF schema renamed with prefix "new_" to each field name
   */
  def joinOldAndNew(newDF: DataFrame, newSchema: StructType, oldDF: DataFrame, oldSchema: StructType, keys: List[(String, String)], joinType: String): DataFrame = {
    if (keys.length < 1) return null

    val oldNullSafe = if (null == oldDF) Spark.getSqlContext().createDataFrame(Spark.getContext().emptyRDD[Row], oldSchema) else oldDF.dropDuplicates().na.drop("any", (keys map (_._2.toString)).toArray)
    val newNullSafe = if (null == newDF) Spark.getSqlContext().createDataFrame(Spark.getContext().emptyRDD[Row], newSchema) else newDF.dropDuplicates().na.drop("any", (keys map (_._1.toString)).toArray)
    joinOldAndNewNonNull(newNullSafe, oldNullSafe, keys, joinType)
  }

  /**
   * each Dataframes should not be null
   * @param newNonNullDF
   * @param oldNonNullDF
   * @param keys
   * @param joinType
   * @return
   */
  def joinOldAndNewNonNull(newNonNullDF: DataFrame, oldNonNullDF: DataFrame, keys: List[(String, String)], joinType: String): DataFrame = {
    assert(null != newNonNullDF && null != oldNonNullDF)
    val bCastOldNullSafe = Spark.getContext().broadcast(oldNonNullDF).value
    val newDFNullSafeRenamed = Spark.getContext().broadcast(SchemaUtils.renameCols(newNonNullDF, NEW_)).value

    val joinExpr: Column = keys.slice(1, keys.size).foldLeft(bCastOldNullSafe(keys(0)._1) === newDFNullSafeRenamed(NEW_ + keys(0)._2))((m: Column, n: (String, String)) => m && bCastOldNullSafe(n._1) === newDFNullSafeRenamed(NEW_ + n._2))
    val result = bCastOldNullSafe.join(newDFNullSafeRenamed, joinExpr, joinType)
    result
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

    dfIncrVar = SchemaUtils.renameCols(dfIncrVar, NEW_)

    // join old and new data frame on primary key
    val joinedDF = dfPrevVarFull.dropDuplicates().join(dfIncrVar, dfPrevVarFull(primaryKey) === dfIncrVar(NEW_ + primaryKey), SQL.FULL_OUTER)

    joinedDF
  }
}
