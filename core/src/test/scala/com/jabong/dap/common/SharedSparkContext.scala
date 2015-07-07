package com.jabong.dap.common

import org.apache.spark._
import org.scalatest.{ BeforeAndAfterAll, Suite }

/** Shares a local `SparkContext` between all tests in a suite and closes it at the end */
trait SharedSparkContext extends BeforeAndAfterAll { self: Suite =>

  val conf = new SparkConf().setMaster("local").setAppName("test").set("spark.driver.allowMultipleContexts", "true")

  override def beforeAll() {
    Spark.init(conf)
    super.beforeAll()
  }

  override def afterAll() {
    //    val sc = Spark.getContext()
    //    if (sc != null) {
    //      sc.stop()
    //    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    //    System.clearProperty("spark.driver.port")
    super.afterAll()
  }
}

