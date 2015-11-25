package com.jabong.dap.data.reader

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.CustomerVariables
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
    val dfReactionCast = dfReaction.select(dfReaction(CustomerVariables.LOGIN_USER_ID) as CustomerVariables.LOGIN_USER_ID,
      dfReaction(CustomerVariables.DEVICE_ID) as CustomerVariables.DEVICE_ID,
      dfReaction(CustomerVariables.MESSAGE_ID) as CustomerVariables.MESSAGE_ID,
      dfReaction(CustomerVariables.CAMPAIGN_ID) as CustomerVariables.CAMPAIGN_ID,
      dfReaction(CustomerVariables.BOUNCE).cast(IntegerType) as CustomerVariables.BOUNCE,
      dfReaction(CustomerVariables.REACTION).cast(IntegerType) as CustomerVariables.REACTION)
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
