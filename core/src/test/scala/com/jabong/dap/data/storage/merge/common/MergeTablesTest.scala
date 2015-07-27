package com.jabong.dap.data.storage.merge.common

import com.jabong.dap.common.SharedSparkContext
import com.jabong.dap.common.constants.variables.SalesCartVariables
import com.jabong.dap.data.acq.common.MergeInfo
import com.jabong.dap.data.storage.DataSets
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

    //    dfIncr = JsonUtils.readFromJson(DataSets.SALES_CART, "1")
    //    dfIncr30 = JsonUtils.readFromJson(DataSets.SALES_CART, "2")
    //    dfPrev = JsonUtils.readFromJson(DataSets.SALES_CART, "2")
  }

  "A Merged DF" should "have size 4" in {
    val mrgInfo = new MergeInfo(source = DataSets.BOB, tableName = DataSets.SALES_CART,
      primaryKey = SalesCartVariables.ID_SALES_CART, mergeMode = DataSets.MONTHLY_MODE,
      dateColumn = Option.apply(SalesCartVariables.UPDATED_AT), incrDate = Option.apply("2015-07-02"),
      fullDate = Option.apply("2015-07-01"), incrMode = Option.apply(DataSets.DAILY_MODE),
      saveMode = DataSets.IGNORE_SAVEMODE)
    MergeTables.merge(mrgInfo)
    //        assert(mergedDF.collect.size == 4)
  }

  //  "Getting the json file" should "give the json file" in {
  //    val inputPath = "/home/pooja/"
  //    val filename = "sales_cart/02"
  //    val filterCond = "fk_customer in (11381529, 9696626, 15508888)"
  //    JsonUtils.writeToJson(inputPath, filename, filterCond)
  //    println("successfully written json File from parquet file at location %s".format(inputPath + filename))
  //  }

}
