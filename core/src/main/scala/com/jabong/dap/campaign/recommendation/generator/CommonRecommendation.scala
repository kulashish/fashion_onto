package com.jabong.dap.campaign.recommendation.generator

import com.jabong.dap.common.{ NullInputException, Spark }
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.Recommendation
import com.jabong.dap.common.constants.status.OrderStatus
import com.jabong.dap.common.constants.variables.{ ProductVariables, SalesOrderItemVariables }
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.data.storage.merge.common.MergeUtils
import grizzled.slf4j.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Row }

/**
 * Created by rahul (first version of basic recommender) on 23/6/15.
 */
class CommonRecommendation extends Logging {
  val sqlContext = Spark.getSqlContext()

  def generateRecommendation(orderData: DataFrame, yesterdayItr: DataFrame): DataFrame = {

    return null

  }

  //  val skuSimpleToSku = udf((skuSimple: String) => simpleToSku(skuSimple: String))
  //
  //  //Converts Sku Simple to Sku
  //  //Input skuSimple:String e.g GE160BG56HMHINDFAS-2211538
  //  //Output GE160BG56HMHINDFAS
  //  def simpleToSku(skuSimple: String): String = {
  //    if (skuSimple == null) {
  //      return null
  //    }
  //    val skuData = skuSimple.split("-")
  //    return skuData(0)
  //  }
  /**
   * top product sold in last 30days
   * @param last30DaysOrderItemData
   * @return
   */
  def topProductsSold(last30DaysOrderItemData: DataFrame): DataFrame = {
    if (last30DaysOrderItemData == null) {
      return null
    }

    val groupedSku = last30DaysOrderItemData.withColumn(Recommendation.SALES_ORDER_ITEM_SKU, Udf.skuFromSimpleSku(last30DaysOrderItemData(SalesOrderItemVariables.SKU)))
      .groupBy(Recommendation.SALES_ORDER_ITEM_SKU)
      //.agg($"actual_sku", count("created_at") as "quantity", max("created_at") as "last_sold_date")
      .agg(count(SalesOrderItemVariables.CREATED_AT) as Recommendation.NUMBER_LAST_30_DAYS_ORDERED, max(SalesOrderItemVariables.CREATED_AT) as Recommendation.LAST_SOLD_DATE)

    return groupedSku
  }

  /**
   * Add weekly average sales for last seven days data
   * @param lastSevenDaysOrderItemsData
   * @return
   */
  def createWeeklyAverageSales(lastSevenDaysOrderItemsData: DataFrame): DataFrame = {
    if (lastSevenDaysOrderItemsData == null) {
      logger.info("order item  dataframe is null")
      return null
    }

    val orderPlacedData = lastSevenDaysOrderItemsData.filter(SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS + " not in ( " + OrderStatus.CANCEL_PAYMENT_ERROR + " , " +
      OrderStatus.CANCELLED + " , " + OrderStatus.INVALID + " , " + OrderStatus.EXPORTABLE_CANCEL_CUST + " , " + OrderStatus.CANCELLED_CC_ITEM + " ) ")

    val orderItemswithWeeklyAverage = orderPlacedData.withColumn(Recommendation.SALES_ORDER_ITEM_SKU, Udf.skuFromSimpleSku(orderPlacedData(SalesOrderItemVariables.SKU)))
      .groupBy(Recommendation.SALES_ORDER_ITEM_SKU)
      .agg(count(Recommendation.SALES_ORDER_ITEM_SKU) / 7 as Recommendation.WEEKLY_AVERAGE_SALE)

    return orderItemswithWeeklyAverage
  }

  /**
   * add weekly average sales to last 30 days data
   * @param weeklyAverageData
   * @param last30OrderItemData
   * @return
   */
  def addWeeklyAverageSales(weeklyAverageData: DataFrame, last30OrderItemData: DataFrame): DataFrame = {
    if (weeklyAverageData == null || last30OrderItemData == null) {
      logger.info("Either weeklyAverageData or last30OrderItemData is null")
      throw new NullInputException("Either weeklyAverageData or last30OrderItemData is null ")
    }

    val updatedWeeklyAverageData = weeklyAverageData.withColumnRenamed(Recommendation.SALES_ORDER_ITEM_SKU, "NEW_" + Recommendation.SALES_ORDER_ITEM_SKU)
    val joinedWeeklyAverageData = last30OrderItemData.join(updatedWeeklyAverageData,
      last30OrderItemData(Recommendation.SALES_ORDER_ITEM_SKU) === updatedWeeklyAverageData("NEW_" + Recommendation.SALES_ORDER_ITEM_SKU), SQL.LEFT_OUTER)

    return joinedWeeklyAverageData
  }

  /**
   *
   * @param topSku
   * @param yesterdayItrData
   * @return
   */
  def skuCompleteData(topSku: DataFrame, yesterdayItrData: DataFrame): DataFrame = {
    if (topSku == null || yesterdayItrData == null) {
      return null
    }
    // import sqlContext.implicits._
    val RecommendationInput = topSku.join(yesterdayItrData, topSku(Recommendation.SALES_ORDER_ITEM_SKU).equalTo(yesterdayItrData(ProductVariables.SKU)), SQL.INNER)
      .select(
        Recommendation.SALES_ORDER_ITEM_SKU,
        ProductVariables.BRICK,
        ProductVariables.MVP,
        ProductVariables.BRAND,
        ProductVariables.CATEGORY,
        ProductVariables.NUMBER_SIMPLE_PER_SKU,
        ProductVariables.GENDER,
        ProductVariables.SPECIAL_PRICE,
        ProductVariables.STOCK,
        Recommendation.NUMBER_LAST_30_DAYS_ORDERED,
        Recommendation.WEEKLY_AVERAGE_SALE,
        Recommendation.LAST_SOLD_DATE)

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

  def genRecommend(recommendationInput: DataFrame, pivotArray: Array[String], dataFrameSchema: StructType, numRecs: Int): DataFrame = {
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
    //val recommendationOutput = mappedRecommendationInput.map{case (key,value) => (key,Array(value))}.reduceByKey(_++_).map{ case (key, value) => (key, genSku(value).toList) }
    //println(recommendationOutput.toDebugString)
    //recommendationOutput.flatMapValues(identity).collect().foreach(println)
    // val recommendations = recommendationOutput.flatMap{case(key,value)=>(value.map( value => (key._1.toString,key._2.asInstanceOf[Long],value._1,value._2.sortBy(-_._1))))}
    val recommendations = recommendationOutput.flatMap{ case (key, value) => (value.map(value => createRow(key, value._1, value._2.sortBy(-_._1).take(numRecs)))) }
    //println(recommendations.toDebugString)
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
  Generate the recommendation based on gender and some group by keys
  Input:- iterable Row od data
  Output: - generate the List of sku for gender
 */
  /**
   *
   * @param iterable
   * @return
   */
  def genSku(iterable: Iterable[Row]): Map[String, scala.collection.mutable.MutableList[(Long, String)]] = {
    var genderSkuMap: Map[String, scala.collection.mutable.MutableList[(Long, String)]] = Map()
    var skuList: scala.collection.mutable.MutableList[(Long, String)] = scala.collection.mutable.MutableList()
    val topRow = iterable.head
    val genderIndex = topRow.fieldIndex(ProductVariables.GENDER)
    val numberSoldIndex = topRow.fieldIndex(Recommendation.NUMBER_LAST_30_DAYS_ORDERED)
    val skuIndex = topRow.fieldIndex(Recommendation.SALES_ORDER_ITEM_SKU)
    for (row <- iterable) {
      val gender = row(genderIndex)
      val quantity = row(numberSoldIndex)
      val sku = row(skuIndex)
      if (gender != null) {
        val recommendedGenderList = RecommendationUtils.getRecommendationGender(gender)
        val recommendGenderArray = recommendedGenderList.split("!")
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

    val recommendGenderX = RecommendationUtils.getRecommendationGender(x(4))
    val recommendGenderY = RecommendationUtils.getRecommendationGender(y(4))
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
    val filteredLastSevenDaysData = RecommendationUtils.daysData(inputDataFrame, timeFrameDays, "last", "last_sold_date")
    val filteredBeforeSevenDaysData = RecommendationUtils.daysData(inputDataFrame, timeFrameDays, "before", "last_sold_date")
    val filteredStock1 = filteredLastSevenDaysData.filter(ProductVariables.STOCK + ">2*" + ProductVariables.WEEKLY_AVERAGE_SALE)

    val filteredStock2 = filteredBeforeSevenDaysData.join(brickBrandStock, filteredBeforeSevenDaysData(ProductVariables.BRAND).equalTo(brickBrandStock("brands"))
      && filteredBeforeSevenDaysData(ProductVariables.BRICK).equalTo(brickBrandStock("bricks")), SQL.INNER)
      .withColumn("stockAvailable", inventoryNotSoldLastWeek(filteredBeforeSevenDaysData(ProductVariables.CATEGORY), filteredBeforeSevenDaysData(ProductVariables.STOCK), brickBrandStock("brickBrandAverage"))).filter("stockAvailable==true")
      .select(ProductVariables.SKU, ProductVariables.BRICK, ProductVariables.MVP, ProductVariables.BRAND,
        ProductVariables.GENDER, ProductVariables.SPECIAL_PRICE, ProductVariables.WEEKLY_AVERAGE_SALE, "quantity", "last_sold_date")

    val outStock = filteredStock1.unionAll(filteredStock2)
    return outStock

  }

  def inventoryCheck(inputData: DataFrame): DataFrame = {
    logger.info("inventory check started")
    if (inputData == null) {
      logger.error(" inputData is null")
      throw new NullInputException("inventoryCheck:- inputData is null")
    }
    val inventoryFiltered = inputData.
      select(
        inventoryChecked(inputData(ProductVariables.CATEGORY), inputData(ProductVariables.NUMBER_SIMPLE_PER_SKU), inputData(ProductVariables.STOCK),
          inputData(Recommendation.WEEKLY_AVERAGE_SALE)) as Recommendation.INVENTORY_FILTER,
        inputData(Recommendation.SALES_ORDER_ITEM_SKU),
        inputData(Recommendation.NUMBER_LAST_30_DAYS_ORDERED),
        inputData(Recommendation.LAST_SOLD_DATE),
        inputData(ProductVariables.CATEGORY),
        inputData(ProductVariables.NUMBER_SIMPLE_PER_SKU),
        inputData(ProductVariables.STOCK),
        inputData(ProductVariables.BRAND),
        inputData(ProductVariables.BRICK),
        inputData(ProductVariables.MVP),
        inputData(ProductVariables.GENDER),
        inputData(ProductVariables.SPECIAL_PRICE)
      )
      .filter(Recommendation.INVENTORY_FILTER + " = true")

    logger.info("inventory check ended")

    return inventoryFiltered
  }

  val inventoryChecked = udf((category: String, numberSkus: Long, stock: Long, weeklyAverage: java.lang.Double) => RecommendationUtils.inventoryFilter(category, numberSkus, stock, weeklyAverage))

  val inventoryNotSoldLastWeek = udf((category: String, stock: Int, weeklyAverage: Int) => RecommendationUtils.inventoryWeekNotSold(category, stock, weeklyAverage))

  def existsInArray(value: String, array: Array[String]): Boolean = {
    for (arrayVal <- array) {
      if (value.equals(arrayVal)) {
        return true
      }
    }
    return false
  }

}

