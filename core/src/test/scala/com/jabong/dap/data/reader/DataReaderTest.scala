package com.jabong.dap.data.reader

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.constants.variables.DevicesReactionsVariables
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.ad4push.schema.DevicesReactionsSchema
import org.apache.spark.sql.types.IntegerType
import org.scalatest.{ Matchers, FlatSpec }

/**
 * Created by Kapil.Rajak on 23/7/15.
 */
class DataReaderTest extends FlatSpec with SharedSparkContext with Matchers {

  "getDataFrame: Data Frame" should "match with expected data" in {
    val dfReaction = DataReader.getDataFrame4mCsv(JsonUtils.TEST_RESOURCES, DataSets.AD4PUSH, DataSets.CSV, DataSets.DAILY_MODE, "2015/07/22", "true", ",")
    val dfReactionCast = dfReaction.select(dfReaction(DevicesReactionsVariables.LOGIN_USER_ID) as DevicesReactionsVariables.LOGIN_USER_ID,
      dfReaction(DevicesReactionsVariables.DEVICE_ID) as DevicesReactionsVariables.DEVICE_ID,
      dfReaction(DevicesReactionsVariables.MESSAGE_ID) as DevicesReactionsVariables.MESSAGE_ID,
      dfReaction(DevicesReactionsVariables.CAMPAIGN_ID) as DevicesReactionsVariables.CAMPAIGN_ID,
      dfReaction(DevicesReactionsVariables.BOUNCE).cast(IntegerType) as DevicesReactionsVariables.BOUNCE,
      dfReaction(DevicesReactionsVariables.REACTION).cast(IntegerType) as DevicesReactionsVariables.REACTION)
    //dfReaction.limit(10).write.json(DataSets.TEST_RESOURCES + "ad4push" + ".json")
    val dfExpected = JsonUtils.readFromJson(DataSets.AD4PUSH, "testDF", DevicesReactionsSchema.schemaCsv)
    assert(dfExpected.collect().toSet.equals(dfReactionCast.collect().toSet))
  }

  "getDataFrame: Data Frame" should "throws IllegalArgumentException for empty basepath" in {
    a[IllegalArgumentException] should be thrownBy {
      DataReader.getDataFrame4mCsv(null, DataSets.AD4PUSH, DataSets.CSV, DataSets.DAILY_MODE, "2015/07/22", "true", ",")
    }
  }

  "getDataFrame: Data Frame" should "throws IllegalArgumentException for empty source" in {
    a[IllegalArgumentException] should be thrownBy {
      DataReader.getDataFrame4mCsv(JsonUtils.TEST_RESOURCES, null, DataSets.CSV, DataSets.DAILY_MODE, "2015/07/22", "true", ",")
    }
  }

  "getDataFrame: Data Frame" should "throws IllegalArgumentException for empty table name" in {
    a[IllegalArgumentException] should be thrownBy {
      DataReader.getDataFrame4mCsv(JsonUtils.TEST_RESOURCES, DataSets.AD4PUSH, null, DataSets.DAILY_MODE, "2015/07/22", "true", ",")
    }
  }

  "getDataFrame: Data Frame" should "throws IllegalArgumentException for empty incr Mode" in {
    a[IllegalArgumentException] should be thrownBy {
      DataReader.getDataFrame4mCsv(JsonUtils.TEST_RESOURCES, DataSets.AD4PUSH, DataSets.CSV, null, "2015/07/22", "true", ",")
    }
  }

  "getDataFrame: Data Frame" should "throws IllegalArgumentException for empty date" in {
    a[IllegalArgumentException] should be thrownBy {
      DataReader.getDataFrame4mCsv(JsonUtils.TEST_RESOURCES, DataSets.AD4PUSH, DataSets.CSV, DataSets.DAILY_MODE, null, "true", ",")
    }
  }
}