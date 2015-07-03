package com.jabong.dap.common.udf

import java.sql.Timestamp

import org.apache.spark.sql.functions._

/**
 * Created by raghu on 3/7/15.
 */
object Udf {

  //udfMinTimestamp will return min of Timestamp t1 or t2
  val udfMinTimestamp = udf((t1: Timestamp, t2: Timestamp) => UdfUtils.getMin(t1: Timestamp, t2: Timestamp))

  //udfMaxTimestamp will return max of Timestamp t1 or t2
  val udfMaxTimestamp = udf((t1: Timestamp, t2: Timestamp) => UdfUtils.getMax(t1: Timestamp, t2: Timestamp))

}
