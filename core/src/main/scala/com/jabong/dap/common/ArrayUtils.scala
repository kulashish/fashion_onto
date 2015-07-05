package com.jabong.dap.common

object ArrayUtils {
  def findIndexInArray(array: Array[String], elem: String): Int = {
    var index: Int = -1
    for (i <- array.indices) {
      if (array(i) == elem) index = i
    }
    index
  }

  def arrayToString(array: Array[Int], index: Int): String = {
    var arrayConverted: String = "";

    for (i <- index to array.length - 1) {
      if (i == index) {
        arrayConverted = array(i).toString
      } else {
        arrayConverted = arrayConverted + "!" + array(i).toString
      }

    }
    return arrayConverted
  }



}
