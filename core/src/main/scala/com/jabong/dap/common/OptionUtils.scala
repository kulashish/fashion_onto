package com.jabong.dap.common

/**
 * Created by pooja on 10/7/15.
 */
object OptionUtils {
  /**
   * Boolean test to check whether a given option string is empty (returns true) or not (returns false).
   */
  def optStringEmpty(strOpt: Option[String]): Boolean = {
    val opt = if (strOpt != null) strOpt.orNull else null
    if (opt == null || opt.length() == 0)
      true
    else
      false
  }

  def getOptValue(strOpt: Option[String]): String = {
    if (null != strOpt)
      strOpt.orNull
    else
      null
  }
}
