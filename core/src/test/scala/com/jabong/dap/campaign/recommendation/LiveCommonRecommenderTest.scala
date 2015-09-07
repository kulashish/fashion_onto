package com.jabong.dap.campaign.recommendation

import com.jabong.dap.common.json.JsonUtils
import com.jabong.dap.common.{SharedSparkContext, Spark, TestSchema}
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by rahul on 4/9/15.
 */
class LiveCommonRecommenderTest extends FlatSpec with SharedSparkContext with Matchers{

  @transient var sqlContext: SQLContext = _
  @transient var recommendationOutput: DataFrame = _
  @transient var refSkusData: DataFrame = _
  var liveRecommender: LiveCommonRecommender = _

  override def beforeAll() {
    super.beforeAll()
    sqlContext = Spark.getSqlContext()
    liveRecommender = new LiveCommonRecommender()
    recommendationOutput = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/recommendation/", "brick_mvp_recommendation_output", Schema.brickMvpRecommendationOutput)
    refSkusData = JsonUtils.readFromJson(DataSets.CAMPAIGNS + "/recommendation/", "ref_sku_data", TestSchema.referenceSku)
  }

  "null reference sku data" should "return IllegalArgumentException" in {
    a[IllegalArgumentException] should be thrownBy{
      liveRecommender.generateRecommendation(null,recommendationOutput)
    }
  }

  "null recommendations  data" should "return IllegalArgumentException" in {
    a[IllegalArgumentException] should be thrownBy{
      liveRecommender.generateRecommendation(refSkusData,null)
    }
  }

  "refskus and brick mvp geneder recommendation  data as input" should "return set of eight recommendedSkus for refSkus" in {
      val expectedRecommendations = liveRecommender.generateRecommendation(refSkusData,recommendationOutput)
    //println("RECOMMENDED"+expectedRecommendations.count())
      //expectedRecommendations.collect().foreach(println)
      assert(expectedRecommendations != null)
    }

  "refSku is null" should "return IllegalArgumentException" in {
    val inputRecommendations:List[(Long,String)] = List((7L,"SO596WA65JLIINDFAS"),(4L,"AD004WA32UCLINDFAS"))
    a[IllegalArgumentException] should be thrownBy {
      liveRecommender.getRecommendedSkus(null,inputRecommendations)
    }
  }

  "recommendation is null" should "return IllegalArgumentException" in {
    val refSku = "AD004WA32UCLINDFAS"
    a[IllegalArgumentException] should be thrownBy {
      liveRecommender.getRecommendedSkus(refSku,null)
    }
  }
  "ref sku AD004WA32UCLINDFAS with two recommendation " should "return ref skus with recommendations" in {
    val refSku = "AD004WA32UCLINDFAS"
    val inputRecommendations:List[(Long,String)] = List((7L,"SO596WA65JLIINDFAS"),(4L,"ES418WA79UAUINDFAS"))
    val expectedValue = liveRecommender.getRecommendedSkus(refSku,inputRecommendations)
    expectedValue.foreach(println)
    assert(expectedValue!=null)
  }


}
