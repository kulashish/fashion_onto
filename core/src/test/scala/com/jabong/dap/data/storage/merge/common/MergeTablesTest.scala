package com.jabong.dap.data.storage.merge.common

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.json.JsonUtils
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by pooja on 24/7/15.
 */
class MergeTablesTest extends FlatSpec with SharedSparkContext {
  @transient var dfIncr: DataFrame = _
  @transient var dfIncr30: DataFrame = _
  @transient var dfPrev: DataFrame = _

  override def beforeAll() {
    super.beforeAll()

    dfIncr = JsonUtils.readFromJson("common/merge", "1")
    dfIncr30 = JsonUtils.readFromJson("common/merge", "2")
    dfPrev = JsonUtils.readFromJson("common/merge", "2")
  }

  "A Merged DF" should "have size 4" in {
//    val mrgInfo = new MergeInfo(source = mergeInfo.source, tableName = mergeInfo.tableName,
//      primaryKey = mergeInfo.primaryKey, mergeMode = mergeInfo.mergeMode, dateColumn = mergeInfo.dateColumn,
//      incrDate = Option.apply(end), fullDate = Option.apply(prevFullDate),
//      incrMode = Option.apply(DataSets.DAILY_MODE), saveMode = DataSets.IGNORE_SAVEMODE)
//    var mergedDF = MergeTables.merge(mrgInfo)
//    assert(mergedDF.collect.size == 4)
  }

}
