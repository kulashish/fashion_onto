package com.jabong.dap.common

import org.scalatest.{ Matchers, FlatSpec }

/**
 * Created by pooja on 28/7/15.
 */
class StringUtilsTest extends FlatSpec with Matchers {

  "isEmpty" should "return true" in {
    StringUtils.isEmpty("") should be (true)
  }

  "isEmpty" should "return false" in {
    StringUtils.isEmpty("2015-05-19") should be (false)
  }

}
