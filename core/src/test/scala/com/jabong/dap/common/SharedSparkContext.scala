package com.jabong.dap.common

import com.jabong.dap.common.json.JsonUtils
import org.apache.spark._
import org.scalatest.{ BeforeAndAfterAll, Suite }

/** Shares a local `SparkContext` between all tests in a suite and closes it at the end */
trait SharedSparkContext extends BeforeAndAfterAll { self: Suite =>

  val conf = new SparkConf().setMaster("local").setAppName("test").set("spark.driver.allowMultipleContexts", "true")

  override def beforeAll() {
    if (Spark.getContext() == null)
      //      Spark.init(conf, "WARN")
      Spark.init(conf)

    val config = new Config(basePath = JsonUtils.TEST_RESOURCES)
    AppConfig.config = config

    super.beforeAll()
  }

  override def afterAll() {
    //    val sc = Spark.getContext()
    //    if (sc != null) {
    //      sc.stop()
    //    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    //System.clearProperty("spark.driver.port")
    super.afterAll()
  }
}

