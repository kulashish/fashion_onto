package com.jabong.dap.model.customer

import com.jabong.dap.common.{ SharedSparkContext }
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.model.customer.data.UUIDGenerator
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by mubarak on 7/10/15.
 */
class UUIDGeneratorTest extends FlatSpec with SharedSparkContext {

  @transient var cmr: DataFrame = _

  override def beforeAll() {
    super.beforeAll()

    cmr = JsonUtils.readFromJson(DataSets.EXTRAS, "cmr_uid")

    //cmr.collect().foreach(println)

  }

  "Testing addUid method 1" should "match the output dataframe" in {

    val res = UUIDGenerator.addUid(cmr)
    res.collect().foreach(println)
    assert(res.collect().length == 5)
  }

}

