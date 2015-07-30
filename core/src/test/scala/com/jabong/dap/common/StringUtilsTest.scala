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

  "isAllZero" should "return true" in {
    StringUtils.isAllZero("09C36952-A674-46BE-9430-C72827F07836") should be (false)
  }

  "isAllZero" should "return false" in {
    StringUtils.isAllZero("0000") should be (true)
  }

}
