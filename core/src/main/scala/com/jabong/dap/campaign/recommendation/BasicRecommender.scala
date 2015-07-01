package com.jabong.dap.campaign.recommendation

import java.util

import com.jabong.dap.common.constants.variables.ProductVariables
import com.jabong.dap.common.Spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.functions._

import scala.collection.SortedSet


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


  def daysData(data:DataFrame,days:Int,Type:String,column:String): DataFrame ={
    var expType:String=""
    if(Type=="last"){
        expType="<"
    }
    else
      expType=">"

    if (data == null || days < 0 || column == null) {
      return null
    }
    val lastDaysData = data.filter("(unix_timestamp() -unix_timestamp("+column+",'yyyy-MM-dd HH:mm:ss.S'))/60/60/24<"+days)
    return lastDaysData
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

    //   orderItemData.select(skuSimpleToSku(orderItemData("slu")) as ("asdxas"),orderItemData("sku"))

    val groupedSku = lastDaysData.withColumn("actual_sku", skuSimpleToSku(lastDaysData("sku"))).groupBy("actual_sku")
      .agg($"actual_sku", count("created_at") as "quantity",max("created_at") as "last_sold_date")

    return groupedSku
  }

  def skuCompleteData(topSku: DataFrame, SkuCompleteData: DataFrame): DataFrame = {
    if (topSku == null || SkuCompleteData == null) {
      return null
    }
    import hiveContext.implicits._


    val RecommendationInput = topSku.join(SkuCompleteData, topSku("actual_sku").equalTo(SkuCompleteData("sku")), "inner")
      .select(ProductVariables.Sku,ProductVariables.Brick, ProductVariables.MVP,  ProductVariables.Brand,
        ProductVariables.Gender,ProductVariables.SpecialPrice,ProductVariables.WeeklyAverageSale,"quantity","last_sold_date")


    return RecommendationInput
  }

  def createRow(row: Row,array: Array[Int]):Row={
    var sequence = new Seq[Int] = 
    for(value <- array){
      sequence+=(value)
    }
    row.get()
    Row.apply()
    return data
  }
  // Input: recommendationInput: contains sorted list of all skus sold in last x days
  //        schema: {sku, brick, mvp, brand, gender, sp, weeklyAverage of number sold}
  // Ouput: (mvp, brick, gender) and its sorted list of recommendations

  def genRecommend(recommendationInput:DataFrame): DataFrame ={
    if(recommendationInput==null){
      return null
    }





//    val pivotArray = Array(1,2)
//    for (pivot <- pivotArray){
//      row => row(pivot)
//    }
    val test =  recommendationInput.rdd.keyBy(row =>createRow(row,pivotArray))


    // val mappedRecommendationInput = recommendationInput.map(row => ((row(1),row(2)),(row(0).toString,row(4).toString,row(3).toString,row(5).asInstanceOf[Int],row(6).asInstanceOf[Int])))
   // val mappedRecommendationInput = recommendationInput.map(row => ((row(1),row(2)),row))
    val mappedRecommendationInput = recommendationInput.map(row => ((row(1),row(2)),row))

    //mappedRecommendationInput.collect().foreach(println)


    //  val recommendationOutput = mappedRecommendationInput.reduceByKey((x,y)=>generateSku(x,y))

    val recommendationOutput = mappedRecommendationInput.groupByKey().map{ case(key,value)=>(key,genSku(value).toList)}
    //recommendationOutput.flatMapValues(identity).collect().foreach(println)
    val recommendations = recommendationOutput.flatMap{case(key,value)=>(value.map( value => (key._1.toString,key._2.asInstanceOf[Int],value._1,value._2.sortBy(-_._1))))}
    val recommendDataFrame = hiveContext.createDataFrame(recommendations)
    recommendations.collect().foreach(println)
   // recommendationOutput.collect().foreach(println)
   // val testrec = recommendationOutput.map{case(key,value)=>(key,value.flatMap(t=>testMap(t)))}
    //testrec.collect().foreach(println)

    return recommendDataFrame
  }


def testMap(x :(String,scala.collection.mutable.MutableList[(Long,String)])) = (x._2)


  def genSku(iterable: Iterable[Row]): Map[String,scala.collection.mutable.MutableList[(Long,String)]]={
    var genderSkuMap : Map[String,scala.collection.mutable.MutableList[(Long,String)]] = Map()
    var skuList : scala.collection.mutable.MutableList[(Long,String)] =scala.collection.mutable.MutableList()
    for(row <- iterable){
      val gender = row(4)
      val quantity = row(7)
      val sku = row(0)
      if(gender!=null){
        val recommendedGenderList = getRecommendationGender(gender)
        val recommendGenderArray = recommendedGenderList.split(",")
        for (recGender <- recommendGenderArray) {
          skuList = genderSkuMap.getOrElse(recGender,null)
          println(gender+"\t"+recGender,skuList)

          if(skuList!=null){
            skuList.+=((quantity.asInstanceOf[Long],sku.toString))
            genderSkuMap += (recGender.toString -> skuList)

          }
          else{
            skuList= scala.collection.mutable.MutableList()
            skuList.+=((quantity.asInstanceOf[Long],sku.toString))
            genderSkuMap += (recGender.toString -> skuList)

          }
        }

      }

    }

    return genderSkuMap
  }



  def generateSku(x:Row,y:Row): Row ={
    if(x ==null || y==null || x(4)==null || y(4)==null){
      return null
    }

    var genderSkuMap : Map[String,Set[String]] = Map()
    var skuList : Set[String] = Set()
    var recommendedRow = Row()
    if(x(4).equals(y(4))){
      skuList += (x(0).toString)
      skuList +=(y(0).toString)
      genderSkuMap+=(x(4).toString -> skuList)
      recommendedRow = Row(x(4),y(4),genderSkuMap)
      return recommendedRow
    }

    val recommendGenderX = getRecommendationGender(x(4))
    val recommendGenderY = getRecommendationGender(y(4))
    if(recommendGenderX !=null) {
      val recommendGenderArray = recommendGenderX.split(",")

      val genderMatches = existsInArray(y(4).toString, recommendGenderArray)
      println(y(4)+"\t"+recommendGenderX+"\t"+genderMatches)
      var skuList = genderSkuMap.getOrElse(x(4).toString,null)
      if(skuList==null){
        skuList = Set()
      }
      if (genderMatches) {

        skuList += (x(0).toString)
        skuList += (y(0).toString)
        println(skuList)
        genderSkuMap += (x(4).toString -> skuList)
      }
      else {
        skuList += (x(0).toString)
        genderSkuMap += (x(4).toString -> skuList)
      }
    }

    if(recommendGenderY!=null) {
      val recommendGenderArray = recommendGenderY.split(",")

      val genderMatches = existsInArray(x(4).toString, recommendGenderArray)
      println(x(4)+"\t"+recommendGenderY+"\t"+genderMatches)
      var skuList = genderSkuMap.getOrElse(y(4).toString,null)
      if(skuList==null){
        skuList = Set()
      }

      if (genderMatches) {

        skuList += (x(0).toString)
        skuList += (y(0).toString)
        println(skuList)
        genderSkuMap += (y(4).toString -> skuList)
      }
      else {
        skuList += (y(0).toString)
        genderSkuMap += (y(4).toString -> skuList)
      }
    }
    recommendedRow = Row(x(4), y(4), genderSkuMap)

    return recommendedRow
  }


  def inventoryFilter(inputDataFrame:DataFrame,timeFrameDays:Int,brickBrandStock:DataFrame): DataFrame ={
    if (inputDataFrame == null || timeFrameDays < 0) {
      return null
    }

    val filteredLastSevenDaysData = daysData(inputDataFrame,timeFrameDays,"last","last_sold_date")

    val filteredBeforeSevenDaysData = daysData(inputDataFrame,timeFrameDays,"before","last_sold_date")


    val filteredStock1 = filteredLastSevenDaysData.filter(ProductVariables.Stock+">2*"+ProductVariables.WeeklyAverageSale)

    val filteredStock2 = filteredBeforeSevenDaysData.join(brickBrandStock,filteredBeforeSevenDaysData(ProductVariables.Brand).equalTo(brickBrandStock("brands"))
      && filteredBeforeSevenDaysData(ProductVariables.Brick).equalTo(brickBrandStock("bricks")) ,"inner")
      .withColumn("stockAvailable",inventoryNotSoldLastWeek(filteredBeforeSevenDaysData(ProductVariables.Category)
      ,filteredBeforeSevenDaysData(ProductVariables.Stock),brickBrandStock("brickBrandAverage"))).filter("stockAvailable==true")
    .select(ProductVariables.Sku,ProductVariables.Brick, ProductVariables.MVP,  ProductVariables.Brand,
        ProductVariables.Gender,ProductVariables.SpecialPrice,ProductVariables.WeeklyAverageSale,"quantity","last_sold_date")

    return filteredStock1.unionAll(filteredStock2)

  }

  val inventoryNotSoldLastWeek = udf((category:String,stock:Int,weeklyAverage:Int) => inventoryWeekNotSold(category,stock,weeklyAverage))




  def existsInArray(value:String,array: Array[String]): Boolean = {
    for (arrayVal <- array) {
      if(value.equals(arrayVal)){
        return true
      }
    }
    return false
  }

}



