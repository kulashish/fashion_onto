package com.jabong.dap.data.storage.merge.common

/**
 * Gets the Path for the base and incremental dataFrames
 */
object MergePathResolver {

  def basePathResolver (pathFull: String ): String ={
    if (DataVerifier.hdfsDataExists(pathFull)) {
      pathFull
    } else {
      throw new DataNotFound("Base Data not found for specified inputs")
    }
  }

  def incrementalPathResolver(pathYesterdayData: String): String = {
    if (DataVerifier.hdfsDataExists(pathYesterdayData)) {
      pathYesterdayData
    } else {
      throw new DataNotFound("Incremental Data not found for specified inputs")
    }
  }

}

case class DataNotFound(message: String) extends Exception
