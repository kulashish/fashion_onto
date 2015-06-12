package com.jabong.dap.common

object ArrayUtils {
  def findIndexInArray(array: Array[String], elem: String): Int =  {
    var index: Int = -1;
    for (i <- 0 to array.length - 1) {
      if (array(i) == elem) index = i
    }
    index
  }
}
