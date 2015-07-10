package com.jabong.dap.common

import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by pooja on 10/7/15.
 */
class OptionUtilsTest extends FlatSpec with Matchers {

  "optStringEmpty" should "return true" in {
    val opt = Option.empty[String]
    OptionUtils.optStringEmpty(opt) should be (true)
  }

  "optStringEmpty" should "return false" in {
    val opt = Option.apply("2015-06-19")
    OptionUtils.optStringEmpty(opt) should be (false)
  }
}
