package com.jabong.dap.common.udf

import java.sql.Timestamp
import org.scalatest.FlatSpec

/**
 * Created by raghu on 3/7/15.
 */
class UdfUtilsTest extends FlatSpec {

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Name of variable: ACC_REG_DATE
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  "getMin(): timestamp t1 and t2 value " should "be null" in {

    val t1 = null

    val t2 = null

    val result = UdfUtils.getMin(t1, t2)

    assert(result == null)

  }

  "getMin(): timestamp t1" should "be null" in {

    val t1 = null

    val t2 = Timestamp.valueOf("2015-04-30 00:05:07.0")

    val result = UdfUtils.getMin(t1, t2)

    assert(result.compareTo(t2) == 0)

  }

  "getMin(): timestamp t2" should "be null" in {

    val t1 = Timestamp.valueOf("2015-04-30 00:05:07.0")

    val t2 = null

    val result = UdfUtils.getMin(t1, t2)

    assert(result.compareTo(t1) == 0)

  }

  "getMin(): return timestamp " should "t1" in {

    val t1 = Timestamp.valueOf("2015-04-30 00:05:07.0")

    val t2 = Timestamp.valueOf("2015-04-30 00:05:09.0")

    val result = UdfUtils.getMin(t1, t2)

    assert(result.compareTo(t1) >= 0)

  }

  "getMin(): return timestamp " should "t2" in {

    val t1 = Timestamp.valueOf("2015-04-30 00:05:09.0")

    val t2 = Timestamp.valueOf("2015-04-30 00:05:07.0")

    val result = UdfUtils.getMin(t1, t2)

    assert(result.compareTo(t2) >= 0)

  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // Name of variable: MAX_UPDATED_AT
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  "getMax(): timestamp t1 and t2 value " should "be null" in {

    val t1 = null

    val t2 = null

    val result = UdfUtils.getMax(t1, t2)

    assert(result == null)

  }

  "getMax(): timestamp t1" should "be null" in {

    val t1 = null

    val t2 = Timestamp.valueOf("2015-04-30 00:05:07.0")

    val result = UdfUtils.getMax(t1, t2)

    assert(result.compareTo(t2) == 0)

  }

  "getMax(): timestamp t2" should "be null" in {

    val t1 = Timestamp.valueOf("2015-04-30 00:05:07.0")

    val t2 = null

    val result = UdfUtils.getMax(t1, t2)

    assert(result.compareTo(t1) == 0)

  }

  "getMax(): return timestamp " should "t2" in {

    val t1 = Timestamp.valueOf("2015-04-30 00:05:07.0")

    val t2 = Timestamp.valueOf("2015-04-30 00:05:09.0")

    val result = UdfUtils.getMax(t1, t2)

    assert(result.compareTo(t2) == 0)

  }

  "getMax(): return timestamp " should "t1" in {

    val t1 = Timestamp.valueOf("2015-04-30 00:05:09.0")

    val t2 = Timestamp.valueOf("2015-04-30 00:05:07.0")

    val result = UdfUtils.getMax(t1, t2)

    assert(result.compareTo(t1) == 0)

  }

}
