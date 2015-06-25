package com.jabong.dap.campaign.recommendation

import com.jabong.dap.common.Constants.Variables.ProductVariables
import com.jabong.dap.common.Spark
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.functions._


/**
 * Created by jabong1145 on 23/6/15.
 */
class BasicRecommender extends Recommender{
  val hiveContext = Spark.getHiveContext()


  def generateRecommendation(orderData: DataFrame): DataFrame = {

    return null

  }


  val skuSimpleToSku = udf((skuSimple: String) => simpleToSku(skuSimple: String))

//  val daysDiff = udf((date: Timestamp) => simpleToSku(skuSimple: String))




  //Converts Sku Simple to Sku
  //Input skuSimple:String e.g GE160BG56HMHINDFAS-2211538
  //Output GE160BG56HMHINDFAS
  def simpleToSku(skuSimple: String): String = {
    if (skuSimple == null) {
      return null
    }
    val skuData = skuSimple.split("-")
    return skuData(0)
  }

  def topProductsSold(orderItemData: DataFrame, days: Int): DataFrame = {
    if (orderItemData == null || days < 0) {
      return null
    }
    import hiveContext.implicits._


    val lastDaysData = orderItemData.filter("(unix_timestamp() -unix_timestamp(created_at,'yyyy-MM-dd HH:mm:ss.S'))/60/60/24<"+days)

    if(lastDaysData.count()==0){
      return null
    }

    //orderItemData.select(skuSimpleToSku() as ("asdxas"),orderItemData("sku"))

    val groupedSku = lastDaysData.withColumn("actual_sku", skuSimpleToSku(lastDaysData("sku"))).groupBy("actual_sku")
      .agg($"actual_sku", count("created_at") as "quantity").orderBy($"quantity".desc)
    groupedSku.collect().foreach(println)

    return groupedSku
  }

  def skuCompleteData(topSku: DataFrame, SkuCompleteData: DataFrame): DataFrame = {
    if (topSku == null || SkuCompleteData == null) {
      return null
    }

    val RecommendationInput = topSku.join(SkuCompleteData, topSku("actual_sku").equalTo(SkuCompleteData("sku")), "inner")
        .select(ProductVariables.Sku,ProductVariables.Brick, ProductVariables.MVP,  ProductVariables.Brand,
        ProductVariables.Gender,ProductVariables.SpecialPrice,ProductVariables.WeeklyAverageSale)


    return RecommendationInput
  }

  // Input: recommendationInput: contains sorted list of all skus sold in last x days
  //        schema: {sku, brick, mvp, brand, gender, sp, weeklyAverage of number sold}
  // Ouput: (mvp, brick, gender) and its sorted list of recommendations

  def genRecommend(recommendationInput:DataFrame): DataFrame ={
    if(recommendationInput==null){
      return null
    }

   // val mappedRecommendationInput = recommendationInput.map(row => ((row(1),row(2)),(row(0).toString,row(4).toString,row(3).toString,row(5).asInstanceOf[Int],row(6).asInstanceOf[Int])))
    val mappedRecommendationInput = recommendationInput.map(row => ((row(1),row(2)),row))

    val recommendationOutput = mappedRecommendationInput.reduceByKey((x,y)=>generateSku(x,y))
    recommendationOutput.collect().foreach(println)
    return null
  }

  def generateSku(x:Row,y:Row): Row ={

    if(x(4) ==null || y(4)==null){
      return null
    }
    var genderSkuMap : Map[String][Set] = Map()
    var skuList : Set[String] = Set()
    var recommendedRow = Row()
    val recommendGenderX = getRecommendationGender(x(4))
    val recommendGenderY = getRecommendationGender(y(4))

    println("HELOOOOOOOOOOOOOOOO"+recommendGenderX+"\t"+x(4)+"\t"+y(4))
    if(recommendGenderX !=null){
      val recommendGenderArray = recommendGenderX.split(",")

      if(x(4)==y(4)){


        skuList += (x(0).toString)
        skuList +=(y(0).toString)
        genderSkuMap+=(x(4).toString -> skuList)
        recommendedRow = Row(genderSkuMap)
        return

      }
      //if(x(7)==null){

     // }
      for(gender <- recommendGenderArray){
        if(gender.equals(y(4))){
          println("HELOOOOOOOOOOOOOOOO"+recommendGenderX+"\t"+x(0)+"\t"+y(0))

          skuList += (x(0).toString)
          skuList +=(y(0).toString)
          println(skuList)

          //recommendedRow = Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6))
          recommendedRow = Row(x(4),y(4),skuList)
          println(recommendedRow)
          return recommendedRow
        }
      }
    }
    if(recommendGenderY !=null){
      val recommendGenderArray = recommendGenderY.split(",")
      //if(x(7)==null){

      // }
      for(gender <- recommendGenderArray){
        if(gender.equals(y(4))){
          skuList += (x(0).toString)
          skuList +=(y(0).toString)
          println(skuList)

          //recommendedRow = Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6))
          recommendedRow = Row(x(4),y(4),skuList)
          println(recommendedRow)
          return recommendedRow
        }
      }
    }



    return recommendedRow
  }

}



