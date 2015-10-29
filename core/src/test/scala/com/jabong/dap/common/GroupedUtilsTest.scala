package com.jabong.dap.common

import com.jabong.dap.common.constants.variables.{ CustomerVariables, ProductVariables }
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.types.{ IntegerType, DecimalType }
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.scalatest.FlatSpec

/**
 * Created by rahul on 29/10/15.
 */
class GroupedUtilsTest extends FlatSpec with SharedSparkContext {
  @transient var refSkuInputPush: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    refSkuInputPush = JsonUtils.readFromJson(DataSets.CAMPAIGNS, "ref_sku_input_push", TestSchema.refSkuInput)
  }

  "Given refSKuInput and grouped fields as fk_customer and order field as price with ascending order" should "return  min price sku ordered field" in {
    val groupFields = Array(CustomerVariables.FK_CUSTOMER)
    val aggFields = Array(CustomerVariables.FK_CUSTOMER, ProductVariables.SPECIAL_PRICE, ProductVariables.SKU_SIMPLE, ProductVariables.MVP, ProductVariables.GENDER, ProductVariables.BRICK, ProductVariables.BRAND)
    val orderField = ProductVariables.SPECIAL_PRICE
    val testData = GroupedUtils.orderGroupBy(refSkuInputPush, groupFields, aggFields, "first", TestSchema.groupTestOut, ProductVariables.SPECIAL_PRICE, "ASC", DecimalType.apply())
    val outputCount = testData.filter(CustomerVariables.FK_CUSTOMER + " = 8552648 and " + ProductVariables.SPECIAL_PRICE + " = 1095.00")
    assert(outputCount.count == 1)
  }

  "Given refSKuInput and grouped fields as fk_customer and order field as price with descending order" should "return  max price sku ordered field" in {
    val groupFields = Array(CustomerVariables.FK_CUSTOMER)
    val aggFields = Array(CustomerVariables.FK_CUSTOMER, ProductVariables.SPECIAL_PRICE, ProductVariables.SKU_SIMPLE, ProductVariables.MVP, ProductVariables.GENDER, ProductVariables.BRICK, ProductVariables.BRAND)
    val orderField = ProductVariables.SPECIAL_PRICE
    val testData = GroupedUtils.orderGroupBy(refSkuInputPush, groupFields, aggFields, "first", TestSchema.groupTestOut, ProductVariables.SPECIAL_PRICE, "DESC", DecimalType.apply())
    val outputCount = testData.filter(CustomerVariables.FK_CUSTOMER + " = 8552648 and " + ProductVariables.SPECIAL_PRICE + " = 2095.00")
    assert(outputCount.count == 1)
  }

}

