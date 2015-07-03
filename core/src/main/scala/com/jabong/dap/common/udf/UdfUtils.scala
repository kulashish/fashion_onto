package com.jabong.dap.common.udf

import java.sql.Timestamp

/**
 * Created by raghu on 3/7/15.
 */
object UdfUtils {

  //min of Timestamp t1 or t2
  def getMin(t1: Timestamp, t2: Timestamp): Timestamp = {

    if (t1 == null) {
      return t2
    }

    if (t2 == null) {
      return t1
    }

    if (t1.compareTo(t2) >= 0)
      t1
    else
      t2

  }

  //max of Timestamp t1 or t2
  def getMax(t1: Timestamp, t2: Timestamp): Timestamp = {

    if (t1 == null) {
      return t2
    }

    if (t2 == null) {
      return t1
    }

    if (t1.compareTo(t2) < 0)
      t2
    else
      t1

  }
}
