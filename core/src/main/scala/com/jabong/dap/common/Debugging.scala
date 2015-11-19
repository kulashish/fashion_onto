package com.jabong.dap.common

import org.apache.spark.sql.DataFrame

import scala.annotation.elidable
import scala.annotation.elidable._

/**
 * Created by pooja on 18/11/15.
 */
object Debugging {

  @elidable(FINE) def debug(data: DataFrame, name: String) {
    println("Count of " + name + ":-" + data.count() + "\n")
    data.printSchema()
    data.show(10)
  }

}
