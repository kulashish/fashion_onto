package com.jabong.dap.common

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{ SparkConf, SparkContext }

/**
 * Application singleton Context. Every component
 * will use this class to access spark context.
 */
object Spark {
  private var sc: SparkContext = null
  private var sqlContext: SQLContext = null
  private var hiveContext: HiveContext = null

  /**
   * Initialize spark context as well as sqlContext instances
   * @param sConf SparkConf
   */
  def init(sConf: SparkConf) {
    sc = new SparkContext(sConf)
    sqlContext = new SQLContext(sc)
    hiveContext = new HiveContext(sc)
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

  /**
   * Return application specific hive context instance
   * @return HiveContext
   */
  def getHiveContext(): HiveContext = {

    hiveContext
  }
}
