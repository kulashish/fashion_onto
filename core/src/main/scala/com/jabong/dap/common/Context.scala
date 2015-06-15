package com.jabong.dap.common

import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkConf, SparkContext }

/**
 * Application singleton Context. Every component
 * will use this class to access spark context.
 */
object Context {
  private var sc: SparkContext = null
  private var sqlContext: SQLContext = null

  /**
   * Initialize spark context as well as sqlContext instances
   * @param sConf SparkConf
   */
  def init(sConf: SparkConf) {
    sc = new SparkContext(sConf)
    sqlContext = new org.apache.spark.sql.SQLContext(sc)
  }

  /**
   * Return application specific spark context instance
   * @return SparkContext
   */
  def getContext(): SparkContext = {
    sc
  }

  /**
   * Return application specific sql context instance
   * @return SQLContext
   */
  def getSqlContext(): SQLContext = {
    sqlContext
  }
}
