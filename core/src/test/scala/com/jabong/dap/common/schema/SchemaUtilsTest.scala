package com.jabong.dap.common.schema

import com.jabong.dap.data.storage.schema.Schema
import org.scalatest.{ Matchers, FlatSpec }

/**
 * Created by pooja on 28/7/15.
 */
class SchemaUtilsTest extends FlatSpec with Matchers {

  "isSchemaEqual" should "return false" in {
    SchemaUtils.isSchemaEqual(Schema.customer, Schema.nls) should be (false)
  }

}
