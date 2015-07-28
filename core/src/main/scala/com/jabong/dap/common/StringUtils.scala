package com.jabong.dap.common

/**
 * Created by pooja on 28/7/15.
 */
object StringUtils {

  def isEmpty(str: String): Boolean = {
    if (null != str && 0 < str.length)
      false
    else
      true
  }

}
