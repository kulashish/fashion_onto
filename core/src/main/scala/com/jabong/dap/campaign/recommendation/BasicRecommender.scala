package com.jabong.dap.campaign.recommendation

import java.sql.Timestamp

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.variables.{SalesOrderItemVariables, ProductVariables, SalesOrderVariables}
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.common.udf.Udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Row }

/**
 * Created by rahul (first version of basic recommender) on 23/6/15.
 */
class BasicRecommender extends Recommender {
  val sqlContext = Spark.getSqlContext()

  override def generateRecommendation(orderData: DataFrame, yesterdayItr: DataFrame): DataFrame = {

    return null

  }

  val skuSimpleToSku = udf((skuSimple: String) => simpleToSku(skuSimple: String))

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
    if (orderItemData == null || days <= 0) {
      return null
    }
    // import sqlContext.implicits._
    // FIXME:Cross check whether lastDayTimeDifference udf is working fine
    val ndaysOldTime = Timestamp.valueOf(TimeUtils.getDateAfterNDays(-days, TimeConstants.DATE_TIME_FORMAT_MS))
    val yesterdayOldTime = Timestamp.valueOf(TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_TIME_FORMAT_MS))
    val ndaysOldStartTime = TimeUtils.getStartTimestampMS(ndaysOldTime)
    val yesterdayOldEndTime = TimeUtils.getEndTimestampMS(yesterdayOldTime)

    val lastDaysData = CampaignUtils.getTimeBasedDataFrame(orderItemData, SalesOrderVariables.CREATED_AT, ndaysOldStartTime.toString, yesterdayOldEndTime.toString)

    //    val lastDaysData = orderItemData.withColumn("daysPresent", CampaignUtils.lastDayTimeDifferenceString(orderItemData(SalesOrderVariables.CREATED_AT)))
//      .filter("daysPresent<=" + days)

    if (lastDaysData.count() == 0) {
      return null
    }
    val groupedSku = lastDaysData.withColumn("actual_sku", Udf.skuFromSimpleSku(lastDaysData(SalesOrderItemVariables.SKU))).groupBy("actual_sku")
      //.agg($"actual_sku", count("created_at") as "quantity", max("created_at") as "last_sold_date")
      .agg(count(SalesOrderItemVariables.CREATED_AT) as "quantity", max(SalesOrderItemVariables.CREATED_AT) as "last_sold_date")

    return groupedSku
  }

  def skuCompleteData(topSku: DataFrame, SkuCompleteData: DataFrame): DataFrame = {
    if (topSku == null || SkuCompleteData == null) {
      return null
    }
    // import sqlContext.implicits._
    val RecommendationInput = topSku.join(SkuCompleteData, topSku("actual_sku").equalTo(SkuCompleteData(ProductVariables.SKU)), SQL.INNER)
      .select(ProductVariables.SKU, ProductVariables.BRICK, ProductVariables.MVP, ProductVariables.BRAND,
        ProductVariables.GENDER, ProductVariables.SPECIAL_PRICE, "quantity", "last_sold_date")

    return RecommendationInput
  }

  /*
  Given a row  and fields in that row it will return new row with only those keys
  input:- row  and fields: field array
  @returns row with only those fields
   */
  def createKey(row: Row, fields: Array[String]): Row = {
    if (row == null || fields == null || fields.length == 0) {
      return null
    }
    var sequence: Seq[Any] = Seq()
    for (field <- fields) {
      try {
        sequence = sequence :+ (row(row.fieldIndex(field)))
      } catch {
        case ex: IllegalArgumentException => {
          ex.printStackTrace()
          return null
        }

      }
    }
    val data = Row.fromSeq(sequence)
    return data
  }

  /*
    Given a row  and fields in that row it will return new row with only those keys
    input:- row  and fields: field array
    @returns String
     */
  def createKeyString(row: Row, array: Array[Int]): String = {
    var sequence: String = ""
    var i: Int = 0
    for (value <- array) {
      if (i == 0) {
        sequence = sequence + row(value)
      }
      sequence = sequence + "^" + row(value)
    }
    return sequence
  }
  // Input: recommendationInput: contains sorted list of all skus sold in last x days
  //        schema: {sku, brick, mvp, brand, gender, sp, weeklyAverage of number sold}
  // Ouput: (mvp, brick, gender) and its sorted list of recommendations

  def genRecommend(recommendationInput: DataFrame, pivotArray: Array[String], dataFrameSchema: StructType): DataFrame = {
    if (recommendationInput == null || pivotArray == null || dataFrameSchema == null) {
      return null
    }
    val mappedRecommendationInput = recommendationInput.rdd.keyBy(row => createKey(row, pivotArray))
    //val mappedRecommendationInput = recommendationInput.map(row => ((row(1),row(2)),row))
    if (mappedRecommendationInput == null || mappedRecommendationInput.keys == null) {
      return null
    }
    //mappedRecommendationInput.collect().foreach(println)

    // import sqlContext.implicits._
    //  val recommendationOutput = mappedRecommendationInput.reduceByKey((x,y)=>generateSku(x,y))
    val recommendationOutput = mappedRecommendationInput.groupByKey().map{ case (key, value) => (key, genSku(value).toList) }
   // val recommendationOutput = mappedRecommendationInput.map{case (key,value) => (key,Array(value))}.reduceByKey(_++_).map{ case (key, value) => (key, genSku(value).toList) }
    println(recommendationOutput.toDebugString)
    //recommendationOutput.flatMapValues(identity).collect().foreach(println)
    // val recommendations = recommendationOutput.flatMap{case(key,value)=>(value.map( value => (key._1.toString,key._2.asInstanceOf[Long],value._1,value._2.sortBy(-_._1))))}
    val recommendations = recommendationOutput.flatMap{ case (key, value) => (value.map(value => createRow(key, value._1, value._2.sortBy(-_._1)))) }
    println(recommendations.toDebugString)
    val recommendDataFrame = sqlContext.createDataFrame(recommendations, dataFrameSchema)
    return recommendDataFrame
  }

  /*
   Input :- row and array of elements
   Output:- Create a new merged row
   */
  def createRow(row: Row, any: Any*): Row = {
    val seq = row.toSeq
    val newSeq = seq ++ any
    return Row.fromSeq(newSeq)
  }

  /*
  Generate the recommendaation based on gender and some group by keys
  Input:- iterable Row od data
  Output: - generate the List of sku for gender
 */

  def genSku(iterable: Iterable[Row]): Map[String, scala.collection.mutable.MutableList[(Long, String)]] = {
    var genderSkuMap: Map[String, scala.collection.mutable.MutableList[(Long, String)]] = Map()
    var skuList: scala.collection.mutable.MutableList[(Long, String)] = scala.collection.mutable.MutableList()
    for (row <- iterable) {
      val gender = row(4)
      val quantity = row(7)
      val sku = row(0)
      if (gender != null) {
        val recommendedGenderList = getRecommendationGender(gender)
        val recommendGenderArray = recommendedGenderList.split(",")
        for (recGender <- recommendGenderArray) {
          skuList = genderSkuMap.getOrElse(recGender, null)
          if (skuList != null) {
            skuList.+=((quantity.asInstanceOf[Long], sku.toString))
            genderSkuMap += (recGender.toString -> skuList)

          } else {
            skuList = scala.collection.mutable.MutableList()
            skuList.+=((quantity.asInstanceOf[Long], sku.toString))
            genderSkuMap += (recGender.toString -> skuList)

          }
        }

      }

    }

    return genderSkuMap
  }

  def generateSku(x: Row, y: Row): Row = {
    if (x == null || y == null || x(4) == null || y(4) == null) {
      return null
    }

    var genderSkuMap: Map[String, Set[String]] = Map()
    var skuList: Set[String] = Set()
    var recommendedRow = Row()
    if (x(4).equals(y(4))) {
      skuList += (x(0).toString)
      skuList += (y(0).toString)
      genderSkuMap += (x(4).toString -> skuList)
      recommendedRow = Row(x(4), y(4), genderSkuMap)
      return recommendedRow
    }

    val recommendGenderX = getRecommendationGender(x(4))
    val recommendGenderY = getRecommendationGender(y(4))
    if (recommendGenderX != null) {
      val recommendGenderArray = recommendGenderX.split(",")

      val genderMatches = existsInArray(y(4).toString, recommendGenderArray)
      var skuList = genderSkuMap.getOrElse(x(4).toString, null)
      if (skuList == null) {
        skuList = Set()
      }
      if (genderMatches) {

        skuList += (x(0).toString)
        skuList += (y(0).toString)
        println(skuList)
        genderSkuMap += (x(4).toString -> skuList)
      } else {
        skuList += (x(0).toString)
        genderSkuMap += (x(4).toString -> skuList)
      }
    }

    if (recommendGenderY != null) {
      val recommendGenderArray = recommendGenderY.split(",")

      val genderMatches = existsInArray(x(4).toString, recommendGenderArray)
      var skuList = genderSkuMap.getOrElse(y(4).toString, null)
      if (skuList == null) {
        skuList = Set()
      }

      if (genderMatches) {

        skuList += (x(0).toString)
        skuList += (y(0).toString)
        println(skuList)
        genderSkuMap += (y(4).toString -> skuList)
      } else {
        skuList += (y(0).toString)
        genderSkuMap += (y(4).toString -> skuList)
      }
    }
    recommendedRow = Row(x(4), y(4), genderSkuMap)

    return recommendedRow
  }

  def inventoryFilter(inputDataFrame: DataFrame, timeFrameDays: Int, brickBrandStock: DataFrame): DataFrame = {
    if (inputDataFrame == null || timeFrameDays < 0) {
      return null
    }
    val filteredLastSevenDaysData = daysData(inputDataFrame, timeFrameDays, "last", "last_sold_date")
    val filteredBeforeSevenDaysData = daysData(inputDataFrame, timeFrameDays, "before", "last_sold_date")
    val filteredStock1 = filteredLastSevenDaysData.filter(ProductVariables.STOCK + ">2*" + ProductVariables.WEEKLY_AVERAGE_SALE)

    val filteredStock2 = filteredBeforeSevenDaysData.join(brickBrandStock, filteredBeforeSevenDaysData(ProductVariables.BRAND).equalTo(brickBrandStock("brands"))
      && filteredBeforeSevenDaysData(ProductVariables.BRICK).equalTo(brickBrandStock("bricks")), SQL.INNER)
      .withColumn("stockAvailable", inventoryNotSoldLastWeek(filteredBeforeSevenDaysData(ProductVariables.CATEGORY), filteredBeforeSevenDaysData(ProductVariables.STOCK), brickBrandStock("brickBrandAverage"))).filter("stockAvailable==true")
      .select(ProductVariables.SKU, ProductVariables.BRICK, ProductVariables.MVP, ProductVariables.BRAND,
        ProductVariables.GENDER, ProductVariables.SPECIAL_PRICE, ProductVariables.WEEKLY_AVERAGE_SALE, "quantity", "last_sold_date")

    val outStock = filteredStock1.unionAll(filteredStock2)
    return outStock

  }

  val inventoryNotSoldLastWeek = udf((category: String, stock: Int, weeklyAverage: Int) => inventoryWeekNotSold(category, stock, weeklyAverage))

  def existsInArray(value: String, array: Array[String]): Boolean = {
    for (arrayVal <- array) {
      if (value.equals(arrayVal)) {
        return true
      }
    }
    return false
  }

}

