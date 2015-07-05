package com.jabong.dap.common.udf

import java.sql.Timestamp

import com.jabong.dap.common.ArrayUtils

/**
 * Created by raghu on 3/7/15.
 */
object UdfUtils {

  //min of Timestamp t1 or t2
  def getMin(t1: Timestamp, t2: Timestamp): Timestamp = {

    if (t1 == null) {
      return t2
    }

    if (t2 == null) {
      return t1
    }

    if (t1.compareTo(t2) >= 0)
      t1
    else
      t2

  }

  //max of Timestamp t1 or t2
  def getMax(t1: Timestamp, t2: Timestamp): Timestamp = {

    if (t1 == null) {
      return t2
    }

    if (t2 == null) {
      return t1
    }

    if (t1.compareTo(t2) < 0)
      t2
    else
      t1

  }

  //return latest value
  def getLatestValue(a1: Any, a2: Any): Any = {

    if (a2 == null) a1 else a2

  }

  //this will merge two slots data
  def getMergeSlots(oldSlot: Any, newSlot: Any): (String, Int) = {
    if (oldSlot == null && newSlot == null) {
      return null
    }
    if (oldSlot == null) {
      return (newSlot.toString, getMaxSlot(newSlot))
    }
    if (newSlot == null) {
      return (oldSlot.toString, getMaxSlot(oldSlot))
    }
    var max = 0

    var maxSlot = 0
    val oldSlotArray = oldSlot.toString.split("!")
    val newSlotArray = newSlot.toString.split("!")
    var finalSlotArray = new Array[Int](oldSlotArray.length)

    for (i <- 0 to oldSlotArray.length - 1) {
      finalSlotArray(i) = oldSlotArray(i).toInt + newSlotArray(i).toInt
      if (finalSlotArray(i) > max) {
        max = finalSlotArray(i)
        maxSlot = i + 1
      }
    }

    return (ArrayUtils.arrayToString(finalSlotArray, 0), maxSlot)
  }

  //this method will return max value from slot data
  def getMaxSlot(slots: Any): Int = {
    var max = 0
    var maxSlot = 0
    val slotArray = slots.toString.split("!")
    for (i <- 0 to slotArray.length - 1) {
      if (slotArray(i).toInt > max) {
        max = slotArray(i).toInt
        maxSlot = i + 1
      }
    }
    return maxSlot

  }

  //this method will create a slot data
  def getCompleteSlotData(iterable: Iterable[(Int, Int)]): Tuple2[String, Int] = {

    var timeSlotArray = new Array[Int](13)

    var maxSlot: Int = -1

    var max: Int = -1

    iterable.foreach {
      case (slot, value) =>
        if (value > max) { maxSlot = slot; max = value };
        timeSlotArray(slot) = value
    }
    new Tuple2(ArrayUtils.arrayToString(timeSlotArray, 0), maxSlot)
  }

}
