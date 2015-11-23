package com.jabong.dap.model.customer

import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{ SharedSparkContext, TestSchema }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.model.customer.variables.CustomerSegments
import org.apache.spark.sql.DataFrame
import org.scalatest.FlatSpec

/**
 * Created by pooja on 30/9/15.
 */
class CustomerSegmentsTest extends FlatSpec with SharedSparkContext {

  @transient var dfCustomerSegments: DataFrame = _

  override def beforeAll() {

    super.beforeAll()

    dfCustomerSegments = JsonUtils.readFromJson(DataSets.CUSTOMER_SEGMENTS, DataSets.CUSTOMER_SEGMENTS, Schema.customerSegments)

  }

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //Name of variable: fk_customer, MVP, Segment0, Segment1,Segment2, Segment3, Segment4, Segment5, Segment6
  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  "getMvpAndSeg: Data Frame dfCustomerSegments" should "null" in {

    val result = CustomerSegments.getSeg(null)

    assert(result == null)

  }

  "getMvpAndSeg: schema attributes and data type" should
    "match into DataFrame(dfCSH)" in {

      val result = CustomerSegments.getSeg(dfCustomerSegments: DataFrame)

      assert(result != null)

    }

  "getMvpAndSeg: Data Frame" should "match to resultant Data Frame" in {

    val result = CustomerSegments.getSeg(dfCustomerSegments: DataFrame).collect().toSet

    val dfMvpSeg = JsonUtils.readFromJson(DataSets.CUSTOMER_SEGMENTS, "mvp_seg", TestSchema.mvp_seg).collect().toSet

    assert(result.equals(dfMvpSeg))

  }

  //  override def afterAll() {
  //    super.afterAll()
  //  }

}
