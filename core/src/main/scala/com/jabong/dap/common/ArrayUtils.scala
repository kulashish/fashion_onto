package com.jabong.dap.common

object ArrayUtils {
  def findIndexInArray(array: Array[String], elem: String): Int = {
    var index: Int = -1
    for (i <- array.indices) {
      if (array(i) == elem) index = i
    }
    index
  }
}
