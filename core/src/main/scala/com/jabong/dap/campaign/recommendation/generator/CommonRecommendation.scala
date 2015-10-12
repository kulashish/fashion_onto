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
abstract class CommonRecommendation extends Logging {
  val sqlContext = Spark.getSqlContext()

  /**
   * Abstract method to generate recommendations
   * @param orderItemFullData
   * @param yesterdayItrData
   * @param pivotKey
   * @param numRecs
   * @param incrDate
   * @return
   */
  def generateRecommendation(orderItemFullData: DataFrame, yesterdayItrData: DataFrame, pivotKey: String, numRecs: Int, incrDate: String)

  /**
   * top product sold in last 30days
   * @param last30DaysOrderItemData
   * @return
   */
  def productsWithCountSold(last30DaysOrderItemData: DataFrame): DataFrame = {
    if (last30DaysOrderItemData == null) {
      logger.error("last30DaysOrderItemData is null")
      throw new NullInputException("last30DaysOrderItemData is null")
    }

    val groupedSku = last30DaysOrderItemData.withColumn(Recommendation.SALES_ORDER_ITEM_SKU, Udf.skuFromSimpleSku(last30DaysOrderItemData(SalesOrderItemVariables.SKU)))
      .groupBy(Recommendation.SALES_ORDER_ITEM_SKU)
      .agg(count(SalesOrderItemVariables.CREATED_AT) as Recommendation.NUMBER_LAST_30_DAYS_ORDERED,
        max(SalesOrderItemVariables.CREATED_AT) as Recommendation.LAST_SOLD_DATE)

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
   * @param orderItemWithWeeklySale
   * @param yesterdayItrData
   * @return
   */
  def skuCompleteData(orderItemWithWeeklySale: DataFrame, yesterdayItrData: DataFrame): DataFrame = {
    require(orderItemWithWeeklySale != null, "topSku data cannot be null ")
    require(yesterdayItrData != null, "yesterdayItrData  cannot be null ")
    // import sqlContext.implicits._
    val RecommendationInput = orderItemWithWeeklySale.join(yesterdayItrData, orderItemWithWeeklySale(Recommendation.SALES_ORDER_ITEM_SKU).equalTo(yesterdayItrData(ProductVariables.SKU)), SQL.INNER)
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
        ProductVariables.PRICE_BAND,
        ProductVariables.COLOR,
        ProductVariables.DISCOUNT,
        Recommendation.NUMBER_LAST_30_DAYS_ORDERED,
        Recommendation.WEEKLY_AVERAGE_SALE,
        Recommendation.LAST_SOLD_DATE).
        withColumn(Recommendation.DISCOUNT_STATUS, when(col(ProductVariables.DISCOUNT) >= Recommendation.DISCOUNT_THRESHOLD, lit(true)).otherwise(lit(false)))

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
  /**
   *
   * @param recommendationInput
   * @param pivotArray
   * @param dataFrameSchema
   * @param numRecs
   * @return
   */
  def genRecommend(recommendationInput: DataFrame, pivotArray: Array[String], dataFrameSchema: StructType, numRecs: Int): DataFrame = {
    require(recommendationInput != null, "recommendationInput data cannot be null ")
    require(pivotArray != null, "pivotArray  cannot be null ")
    require(dataFrameSchema != null, "dataFrameSchema cannot be null ")
    require(numRecs != 0, "numRecs cannot be zero ")

    recommendationInput.printSchema()
    println(pivotArray(0))
    val mappedRecommendationInput = recommendationInput.rdd.keyBy(row => createKey(row, pivotArray))
    if (mappedRecommendationInput == null || mappedRecommendationInput.keys == null) {
      return null
    }
    //mappedRecommendationInput.collect().foreach(println)

    // import sqlContext.implicits._
    //  val recommendationOutput = mappedRecommendationInput.reduceByKey((x,y)=>generateSku(x,y))
    mappedRecommendationInput.saveAsTextFile("/user/rahulaneja/AlchemyTest/mvp_discount_test_tmp")
    val recommendationOutput = mappedRecommendationInput.groupByKey().map{ case (key, value) => (key, genSku(value).toList) }
    //val recommendationOutput = mappedRecommendationInput.map{case (key,value) => (key,Array(value))}.reduceByKey(_++_).map{ case (key, value) => (key, genSku(value).toList) }
    //println(recommendationOutput.toDebugString)
    //recommendationOutput.flatMapValues(identity).collect().foreach(println)
    // val recommendations = recommendationOutput.flatMap{case(key,value)=>(value.map( value => (key._1.toString,key._2.asInstanceOf[Long],value._1,value._2.sortBy(-_._1))))}
    recommendationOutput.saveAsTextFile("/user/rahulaneja/AlchemyTest/mvp_discount_test")
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
    require(iterable != null, "iterable array cannot be null")
    require(iterable.size > 0, "iterable array length cannot be zero")

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
            //     genderSkuMap += (recGender.toString -> skuList)

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

  /**
   * Inventory check given [sku,category,number_of_simples_per_sku,stock,weekly_average_sale)
   * @param inputData
   * @return
   */
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
        inputData(Recommendation.DISCOUNT_STATUS),
        inputData(ProductVariables.CATEGORY),
        inputData(ProductVariables.NUMBER_SIMPLE_PER_SKU),
        inputData(ProductVariables.STOCK),
        inputData(ProductVariables.BRAND),
        inputData(ProductVariables.BRICK),
        inputData(ProductVariables.MVP),
        inputData(ProductVariables.GENDER),
        inputData(ProductVariables.PRICE_BAND),
        inputData(ProductVariables.COLOR),
        inputData(ProductVariables.SPECIAL_PRICE)
      )
      .filter(Recommendation.INVENTORY_FILTER + " = true")

    logger.info("inventory check ended")

    return inventoryFiltered
  }

  val inventoryChecked = udf((category: String, numberSkus: Long, stock: Long, weeklyAverage: java.lang.Double) => RecommendationUtils.inventoryFilter(category, numberSkus, stock, weeklyAverage))

  //  val inventoryNotSoldLastWeek = udf((category: String, stock: Int, weeklyAverage: Int) => RecommendationUtils.inventoryWeekNotSold(category, stock, weeklyAverage))

  def existsInArray(value: String, array: Array[String]): Boolean = {
    for (arrayVal <- array) {
      if (value.equals(arrayVal)) {
        return true
      }
    }
    return false
  }

}

