package com.jabong.dap.data.writer

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.data.storage.DataSets
import org.scalatest.FlatSpec
import com.jabong.dap.data.write.DataWriter

/**
 * Created by Kapil.Rajak on 23/7/15.
 */
class DataWriterTest extends FlatSpec with SharedSparkContext {
  "writeParquet: Data Frame" should "match with expected data" in {
    //:TODO need to write parquet file and then check it
  }

}
