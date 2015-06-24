package com.jabong.dap.common

import org.scalatest.{ Matchers, FlatSpec }

/**
 * Created by Rachit on 19/6/15.
 */
class ArrayUtilsTest extends FlatSpec with Matchers {
  val array = Array[String]("array", "utils", "test")

  "findIndexInArray" should "return index when element exists" in {
    ArrayUtils.findIndexInArray(array, "array") should be (0)
  }

  "findIndexInArray" should "return -1 when element does not exist" in {
    ArrayUtils.findIndexInArray(array, "abc") should be (-1)
  }
}
