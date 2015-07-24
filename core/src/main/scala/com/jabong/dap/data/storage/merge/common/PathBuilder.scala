package com.jabong.dap.data.storage.merge.common

import java.io.File

import com.jabong.dap.data.storage.DataSets

/**
 * Builds the path for the input data for creating the dataFrames and
 * the path at which the data is to be saved.
 */
object PathBuilder {

  def getPrevDataPath(source: String, tableName: String, mode: String, prevDataDate: String): String = {
    var path = "%s/%s/%s/%s/%s".format(DataSets.INPUT_PATH, source, tableName, mode, prevDataDate)
    if (mode.equals(DataSets.FULL)) {
      path = path + File.separator + "24"
    }
    if (!DataVerifier.dataExists(path)) {
      println("Prev Data Path doesn't exist: " + path)
      throw new DataNotExist
    }
    path
  }

  def getIncrDataPath(incrDate: String, incrDataMode: String, source: String, tableName: String): String = {
    val path = "%s/%s/%s/%s/%s".format(DataSets.INPUT_PATH, source, tableName, incrDataMode, incrDate)
    if (!DataVerifier.dataExists(path)) {
      println("Incr Data Path doesn't exist: " + path)
      throw new DataNotExist
    }
    path
  }

  def getSavePathMerge(source: String, tableName: String, mode: String, incrDate: String): String = {
    mode match {
      case DataSets.FULL =>
        "%s/%s/%s/%s/%s/24".format(DataSets.INPUT_PATH, source, tableName, DataSets.FULL, incrDate)
      case _ =>
        "%s/%s/%s/%s/%s".format(DataSets.INPUT_PATH, source, tableName, mode, incrDate)
    }
  }

  class DataNotExist extends Exception

}
