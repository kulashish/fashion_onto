package com.jabong.dap.data.reader

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.Ad4pushVariables
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.ad4push.schema.Ad4pushSchema
import org.apache.spark.sql.types.IntegerType
import org.scalatest.{ FlatSpec, Matchers }

/**
 * Created by Kapil.Rajak on 23/7/15.
 */
class DataReaderTest extends FlatSpec with SharedSparkContext with Matchers {

  "getDataFrame4mCsv: Data Frame" should "match with expected data" in {
    val dfReaction = DataReader.getDataFrame4mCsv(JsonUtils.TEST_RESOURCES, DataSets.AD4PUSH, DataSets.CSV, DataSets.DAILY_MODE, "2015/07/22", "test.csv", "true", ",")
    val dfReactionCast = dfReaction.select(dfReaction(Ad4pushVariables.LOGIN_USER_ID) as Ad4pushVariables.LOGIN_USER_ID,
      dfReaction(Ad4pushVariables.DEVICE_ID) as Ad4pushVariables.DEVICE_ID,
      dfReaction(Ad4pushVariables.MESSAGE_ID) as Ad4pushVariables.MESSAGE_ID,
      dfReaction(Ad4pushVariables.CAMPAIGN_ID) as Ad4pushVariables.CAMPAIGN_ID,
      dfReaction(Ad4pushVariables.BOUNCE).cast(IntegerType) as Ad4pushVariables.BOUNCE,
      dfReaction(Ad4pushVariables.REACTION).cast(IntegerType) as Ad4pushVariables.REACTION)
    val dfExpected = JsonUtils.readFromJson(DataSets.AD4PUSH, "testDF", Ad4pushSchema.schemaCsv)
    assert(dfExpected.collect().toSet.equals(dfReactionCast.collect().toSet))
  }

  "getDataFrame4mCsv: Data Frame" should "throws IllegalArgumentException for empty basepath" in {
    a[IllegalArgumentException] should be thrownBy {
      DataReader.getDataFrame4mCsv(null, DataSets.AD4PUSH, DataSets.CSV, DataSets.DAILY_MODE, "2015/07/22", "test.csv", "true", ",")
    }
  }

  "getDataFrame4mCsv: Data Frame" should "throws IllegalArgumentException for empty source" in {
    a[IllegalArgumentException] should be thrownBy {
      DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, null, DataSets.CSV, DataSets.DAILY_MODE, "2015/07/22", "test.csv", "true", ",")
    }
  }

  "getDataFrame4mCsv: Data Frame" should "throws IllegalArgumentException for empty table name" in {
    a[IllegalArgumentException] should be thrownBy {
      DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.AD4PUSH, null, DataSets.DAILY_MODE, "2015/07/22", "test.csv", "true", ",")
    }
  }

  "getDataFrame4mCsv: Data Frame" should "throws IllegalArgumentException for empty incr Mode" in {
    a[IllegalArgumentException] should be thrownBy {
      DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.AD4PUSH, DataSets.CSV, null, "2015/07/22", "test.csv", "true", ",")
    }
  }

  "getDataFrame4mCsv: Data Frame" should "throws IllegalArgumentException for empty date" in {
    a[IllegalArgumentException] should be thrownBy {
      DataReader.getDataFrame4mCsv(ConfigConstants.INPUT_PATH, DataSets.AD4PUSH, DataSets.CSV, DataSets.DAILY_MODE, null, "test.csv", "true", ",")
    }
  }
}
