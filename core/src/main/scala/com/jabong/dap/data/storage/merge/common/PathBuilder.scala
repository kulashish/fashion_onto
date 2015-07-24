package com.jabong.dap.data.storage.merge.common

import com.jabong.dap.data.storage.DataSets

/**
 * Builds the path for the input data for creating the dataFrames and
 * the path at which the data is to be saved.
 */
object PathBuilder {

//  val basePath = AppConfig.config.basePath

  def getFullDataPath(fullDataDate: String, source: String, tableName: String): String = {
    val path = "%s/%s/%s/full/%s".format(DataSets.INPUT_PATH, source, tableName, fullDataDate)
    if (!DataVerifier.dataExists(path)) {
      println("Full Data Path doesn't exist: " + path)
      throw new DataNotExist
    }
    path
  }

  def getIncrDataPath(incrDate: String, incrDataMode: String, source: String, tableName: String): String = {
    val path = "%s/%s/%s/%s/%s".format(DataSets.INPUT_PATH, source, tableName, incrDataMode, incrDate)
    if (!DataVerifier.dataExists(path)) {
      println("Full Data Path doesn't exist: " + path)
      throw new DataNotExist
    }
    path
  }

  def getSavePathFullMerge(incrDate: String, source: String, tableName: String): String = {
    "%s/%s/%s/full/%s/24".format(DataSets.INPUT_PATH, source, tableName, incrDate)
  }

  class DataNotExist extends Exception

}
