package com.jabong.dap.campaign.recommendation

import com.jabong.dap.common.constants.variables.{ CustomerVariables, ProductVariables }
import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{ TestConstants, SharedSparkContext, Spark, TestSchema }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.{ Row, DataFrame, SQLContext }
import org.scalatest.{ Matchers, FlatSpec }

/**
 * Created by rahul on 4/9/15.
 */
class LiveCommonRecommenderTest extends FlatSpec with SharedSparkContext with Matchers {

  @transient var sqlContext: SQLContext = _
  @transient var recommendationOutput: DataFrame = _
  @transient var refSkusData: DataFrame = _
  @transient var newReferenceSkuData: DataFrame = _
  @transient var genRecSkuInput: DataFrame = _

  var liveRecommender: LiveCommonRecommender = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    liveRecommender = new LiveCommonRecommender()
    recommendationOutput = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/recommendation/", "brick_mvp_recommendation_output", Schema.brickMvpRecommendationOutput)
    newReferenceSkuData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/recommendation/", "ref_sku_data1", TestSchema.finalReferenceSku)
    genRecSkuInput = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/recommendation/", "gen_rec_sku_input", TestSchema.genRecInput)
    refSkusData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/recommendation/", "ref_sku_data", TestSchema.referenceSku)
  }

  "null reference sku data" should "return IllegalArgumentException" in {
    a[IllegalArgumentException] should be thrownBy {
      liveRecommender.generateRecommendation(null, recommendationOutput)
    }
  }

  "null recommendations  data" should "return IllegalArgumentException" in {
    a[IllegalArgumentException] should be thrownBy {
      liveRecommender.generateRecommendation(newReferenceSkuData, null)
    }
  }

  "refskus and brick mvp geneder recommendation  data as input" should "return set of eight recommendedSkus for refSkus" in {
    val expectedRecommendations = liveRecommender.generateRecommendation(newReferenceSkuData, recommendationOutput)
    //println("RECOMMENDED"+expectedRecommendations.count())
    //expectedRecommendations.collect().foreach(println)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
    assert(expectedRecommendations != null)
  }

  //  "refSku is null" should "return IllegalArgumentException" in {
  //    val inputRecommendations: List[Row] = List(Row(7L, "SO596WA65JLIINDFAS"), Row(4L, "ES418WA79UAUINDFAS"))
  //
  //    a[IllegalArgumentException] should be thrownBy {
  //      liveRecommender.getRecommendedSkus(null, inputRecommendations)
  //    }
  //  }
  //
  //  "recommendation is null" should "return IllegalArgumentException" in {
  //    val refSku = "AD004WA32UCLINDFAS"
  //    a[IllegalArgumentException] should be thrownBy {
  //      liveRecommender.getRecommendedSkus(refSku, null)
  //    }
  //  }
  //
  //  "ref sku AD004WA32UCLINDFAS with two recommendation " should "return ref skus with recommendations" in {
  //    val refSku = "AD004WA32UCLINDFAS"
  //    val row1 = Row(7L, "SO596WA65JLIINDFAS")
  //    val row2 = Row(4L, "ES418WA79UAUINDFAS")
  //    val inputRecommendations: List[Row] = List(row1, row2)
  //    val expectedValue = liveRecommender.getRecommendedSkus(refSku, inputRecommendations)
  //    assert(expectedValue.size == 2)
  //  }
  //
  //  "ref sku SO596WA65JLIINDFAS with two recommendation " should "return ref skus with recommendations" in {
  //    val refSku = "SO596WA65JLIINDFAS"
  //    val row1 = Row(7L, "SO596WA65JLIINDFAS")
  //    val row2 = Row(4L, "ES418WA79UAUINDFAS")
  //    val inputRecommendations: List[Row] = List(row1, row2)
  //    val expectedValue = liveRecommender.getRecommendedSkus(refSku, inputRecommendations)
  //    assert(expectedValue.size == 1)
  //  }

  //  "refskus and brick mvp geneder recommendation  data as input" should "return ref skus with max eight recommendations" in {
  //    val expectedValue = liveRecommender.generateRecommendation(newReferenceSkuData, recommendationOutput)
  //    assert(expectedValue !=null)
  //  }

  "iterable row is null " should "return IllegalArgumentException" in {
    a[IllegalArgumentException] should be thrownBy {
      liveRecommender.getRecSkus(null)
    }
  }

  //FIXME: Test cases fix
  //  "one ref sku per customer and only 3 recommended skus " should "return max 3 rec skus for the same ref sku" in {
  //    val refRecSkuInput = genRecSkuInput.filter(TestConstants.TEST_CASE_FILTER + "= 1").map(row => ((row(row.fieldIndex(CustomerVariables.FK_CUSTOMER))), row))
  //      .groupByKey()
  //
  //    val expectedOut = liveRecommender.getRecSkus(refRecSkuInput.first()._2)
  //    assert(expectedOut._2.size == 3)
  //  }
  //
  //  "one ref sku per customer  and more than 8" should "return max 8 rec skus for the same ref sku" in {
  //    val refRecSkuInput = genRecSkuInput.filter(TestConstants.TEST_CASE_FILTER + "= 3").map(row => ((row(row.fieldIndex(CustomerVariables.FK_CUSTOMER))), row))
  //      .groupByKey()
  //
  //    val expectedOut = liveRecommender.getRecSkus(refRecSkuInput.first()._2)
  //    assert(expectedOut._2.size == 8)
  //  }
  //
  //  "two ref sku per customer  and more than 8 in total" should "return max 8 rec skus and no duplicates for the same ref sku" in {
  //    val refRecSkuInput = genRecSkuInput.filter(TestConstants.TEST_CASE_FILTER + "= 2").map(row => ((row(row.fieldIndex(CustomerVariables.FK_CUSTOMER))), row))
  //      .groupByKey()
  //
  //    val expectedOut = liveRecommender.getRecSkus(refRecSkuInput.first()._2)
  //    assert(expectedOut._2.size == 8)
  //  }

}
