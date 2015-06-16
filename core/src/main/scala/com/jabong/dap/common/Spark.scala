package com.jabong.dap.common

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by pooja on 12/6/15.
 */
object Spark {

  private var sc: SparkContext = null

  private var sqlContext:SQLContext = null

  def init(sconf: SparkConf) {

    sc = new SparkContext(sconf)
    sqlContext = new org.apache.spark.sql.SQLContext(sc)

  }

  def getContext():SparkContext = {
    sc
  }

  def getSqlContext():SQLContext = {
    sqlContext
  }

}
