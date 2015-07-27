package com.jabong.dap.data.writer

import java.io.File

import com.jabong.dap.common.SharedSparkContext
import org.scalatest.FlatSpec

/**
 * Created by Kapil.Rajak on 27/7/15.
 */
class DataWriterTest extends FlatSpec with SharedSparkContext {
  val TEST_RESOURCES = "src" + File.separator + "test" + File.separator + "resources"
  "writeCSV: Data Frame" should "match with written data" in {
    //    val dfReaction = DataReader.getDataFrame4mCsv(TEST_RESOURCES, DataSets.AD4PUSH, DataSets.CSV, DataSets.DAILY_MODE, "2015/07/22", "true", ",")
    //    DataWriter.writeCsv(dfReaction.limit(10),TEST_RESOURCES, DataSets.AD4PUSH, "test", DataSets.FULL, "csv","true",",")
    //    val dfReact = DataReader.getDataFrame4mCsv(TEST_RESOURCES, DataSets.AD4PUSH, "test", DataSets.FULL, "csv", "true", ",")
    //    dfReact.collect().toSet.equals(dfReaction.collect().toSet)
  }
}
