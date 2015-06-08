package com.jabong.context

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by geek on 08/06/15.
 */
object Context {
  val conf = new SparkConf().setAppName("Inventory tracking report").setMaster("local[*]")
  val sContext = new SparkContext(conf)
  val sqlContext = new SQLContext(sContext)
}
