package com.jabong.dap.common

import com.jabong.dap.common.constants.variables.{ SalesOrderVariables, ProductVariables, CustomerVariables }
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.{ Row, DataFrame }
import org.apache.spark.sql.types._
import org.scalatest.FlatSpec

/**
 * Created by rahul on 16/11/15.
 */
class UtilsTest extends FlatSpec with SharedSparkContext {
  @transient var refSkuInputPush: DataFrame = _

  override def beforeAll() {
    super.beforeAll()
    refSkuInputPush = JsonUtils.readFromJson(DataSets.CAMPAIGNS, "ref_sku_input_push", TestSchema.refSkuInput)
  }

  "Given refSKuInput and grouped fields as fk_customer and attribute field as brand" should "return  brand map per customer" in {
    val groupFields = Array(CustomerVariables.FK_CUSTOMER)
    val attributeFields = Array(ProductVariables.BRAND)
    val valueFields = Array("count")
    val testSchema = StructType(Array(
      StructField(SalesOrderVariables.FK_CUSTOMER, LongType, true),
      StructField("brand_list", MapType(StringType, StructType(Array(StructField("count", IntegerType, true))), true))))

    val testData = Utils.generateTopMap(refSkuInputPush, groupFields, attributeFields, valueFields, testSchema)
    val outputCount = testData.filter(CustomerVariables.FK_CUSTOMER + " = 8552648").select("brand_list")
    assert(outputCount.count == 1)
  }

  "Given refSKuInput and grouped fields as fk_customer and attribute field as brand and brick and value fields count and sum price" should "return  brand map per customer" in {
    val groupFields = Array(CustomerVariables.FK_CUSTOMER)
    val attributeFields = Array(ProductVariables.BRAND, ProductVariables.BRICK)
    val valueFields = Array("count", "sum_price")
    val testSchema = StructType(Array(
      StructField(SalesOrderVariables.FK_CUSTOMER, LongType, true),
      StructField("brand_list", MapType(StringType, StructType(Array(StructField("count", IntegerType, true), StructField("sum_price", DoubleType, true))), true)),
      StructField("brick_list", MapType(StringType, StructType(Array(StructField("count", IntegerType, true), StructField("sum_price", DoubleType, true))), true))))

    val testData = Utils.generateTopMap(refSkuInputPush, groupFields, attributeFields, valueFields, testSchema)
    val outputCount = testData.filter(CustomerVariables.FK_CUSTOMER + " = 8552648").select("brand_list")
    assert(outputCount.count == 1)
  }

  "Given refSKuInput and grouped fields as fk_customer and attribute field as brand,brick,gender,mvp and value fields count and sum price" should "return  brand map per customer" in {
    val groupFields = Array(CustomerVariables.FK_CUSTOMER)
    val attributeFields = Array(ProductVariables.BRAND, ProductVariables.BRICK, ProductVariables.GENDER, ProductVariables.MVP)
    val valueFields = Array("count", "sum_price")
    val testSchema = StructType(Array(
      StructField(SalesOrderVariables.FK_CUSTOMER, LongType, true),
      StructField("brand_list", MapType(StringType, StructType(Array(StructField("count", IntegerType, true), StructField("sum_price", DoubleType, true))), true)),
      StructField("brick_list", MapType(StringType, StructType(Array(StructField("count", IntegerType, true), StructField("sum_price", DoubleType, true))), true)),
      StructField("gender_list", MapType(StringType, StructType(Array(StructField("count", IntegerType, true), StructField("sum_price", DoubleType, true))), true)),
      StructField("mvp_list", MapType(StringType, StructType(Array(StructField("count", IntegerType, true), StructField("sum_price", DoubleType, true))), true))))

    val testData = Utils.generateTopMap(refSkuInputPush, groupFields, attributeFields, valueFields, testSchema)
    val outputCount = testData.filter(CustomerVariables.FK_CUSTOMER + " = 8552648").select("brand_list")
    assert(outputCount.count == 1)
  }

  "Given prev Brand Map and new Brand Map" should "create updated brand map" in {
    val prevBrandMap = Map("adidas" -> Row(2, 3190.0))
    val newBrandMap = Map("adidas" -> Row(2, 3190.0))

  }

}