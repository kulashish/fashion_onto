package com.jabong.dap.common

/**
 * Created by pooja on 28/7/15.
 */
object StringUtils {

  /**
   * Boolean test to check whether a given date string is empty (returns true) or not (returns false).
   * @param str
   * @return true or false
   */
  def isEmpty(str: String): Boolean = {
    if (null != str && 0 < str.length)
      false
    else
      true
  }

  def isAllZero(str: String): Boolean = {
    (!isEmpty(str)) && str.matches("^[0]*")
  }

  def cleanString(str: String): String = {
    val cleanedString = str.replaceAll("( |-|%|\\$)", "")
    cleanedString.replaceAll("(/|:|\\(|\\)|\\.)", "")
  }

}
