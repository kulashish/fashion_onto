package com.jabong.dap.common.udf

import java.sql.Timestamp
import com.jabong.dap.common.Spark
import org.apache.spark.sql.functions._

/**
 * Created by raghu on 3/7/15.
 */
object Udf {

//  val hiveContext = Spark.getHiveContext()
//  import hiveContext.implicits._
// Define User Defined Functions
//  val sqlContext = Spark.getSqlContext()

  //minTimestamp will return min of Timestamp t1 or t2
  val minTimestamp = udf((t1: Timestamp, t2: Timestamp) => UdfUtils.getMin(t1: Timestamp, t2: Timestamp))

  //maxTimestamp will return max of Timestamp t1 or t2
  val maxTimestamp = udf((t1: Timestamp, t2: Timestamp) => UdfUtils.getMax(t1: Timestamp, t2: Timestamp))

  //udf will return latest Value value
  val latestValue = udf((a1: Any, a2: Any) => UdfUtils.getLatestValue(a1: Any, a2: Any))

  //this udf will merge two slots data
  val mergeSlots = udf((oldSlot: Any, newSlot: Any) => UdfUtils.getMergeSlots(oldSlot: Any, newSlot: Any))

}
