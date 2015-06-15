package com.jabong.dap.common

import org.apache.spark._

import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite

/** Shares a local `SparkContext` between all tests in a suite and closes it at the end */
trait SharedSparkContext extends BeforeAndAfterAll { self: Suite =>

  val conf = new SparkConf().setMaster("local[4]").setAppName("test")

  override def beforeAll() {
    Context.init(conf)
    super.beforeAll()
  }

  override def afterAll() {
    val sc = Context.getContext()
    if (sc != null) {
      sc.stop()
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
    super.afterAll()
  }
}

