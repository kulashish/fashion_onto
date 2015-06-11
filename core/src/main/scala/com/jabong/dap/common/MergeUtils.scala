package com.jabong.dap.common

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD

object MergeUtils {

  def insertOrUpdateMerge(dfBase: DataFrame, dfIncr: DataFrame, primaryKey: String) : RDD[Row] = {

    // rename dfIncr column names with new_ as prefix
    var dfIncrVar = dfIncr;

    val dfSchema = dfIncr.schema
    val numOfColumns = dfSchema.length
    val incrPrimayKeyColumn = ArrayUtils.findIndexInArray(dfIncr.columns, primaryKey)

    dfSchema.foreach(x => dfIncrVar = dfIncrVar.withColumnRenamed(x.name, "new_" + x.name))
    // join on primary key
    val joinedDF = dfBase.join(dfIncrVar, dfBase(primaryKey) === dfIncrVar("new_" + primaryKey), "outer")

    def reduceFunc(x: Row): Row = {
      val hello = x.toSeq.splitAt(numOfColumns)
      if (x(incrPrimayKeyColumn + numOfColumns) == null)
        Row.fromSeq(hello._1)
      else
        Row.fromSeq(hello._2)
    }

    val mergedDF = joinedDF.map(x => reduceFunc(x))
    mergedDF
  }
}

