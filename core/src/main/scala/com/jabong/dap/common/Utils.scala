package com.jabong.dap.common

import java.math.BigDecimal

import com.jabong.dap.campaign.utils.CampaignUtils._
import com.jabong.dap.common.constants.campaign.Recommendation
import com.jabong.dap.common.constants.variables.ProductVariables
import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.common.udf.UdfUtils._
import com.jabong.dap.common.udf.{ Udf, UdfUtils }
import grizzled.slf4j.Logging
import org.apache.spark.sql.{ Row, DataFrame }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
 * Created by mubarak on 21/10/15.
 */
object Utils extends Logging {

  /**
   * Filtered Data based on before time to after Time yyyy-mm-dd HH:MM:SS.s
   * @param inData
   * @param timeField
   * @param after
   * @param before
   * @return
   */
  def getTimeBasedDataFrame(inData: DataFrame, timeField: String, after: String, before: String): DataFrame = {
    if (inData == null || timeField == null || before == null || after == null) {
      logger.error("Any of the value in getTimeBasedDataFrame is null")
      return null
    }

    if (after.length != before.length) {
      logger.error("before and after time formats are different ")
      return null
    }

    val Columns = inData.columns
    if (!(Columns contains (timeField))) {
      logger.error(timeField + "doesn't exist in the inData Frame Schema")
      return null
    }

    val filteredData = inData.filter(timeField + " >= '" + after + "' and " + timeField + " <= '" + before + "'")
    logger.info("Input Data Frame has been filtered before" + before + " after '" + after)
    return filteredData
  }

  def getOneDayData(inData: DataFrame, timeField: String, date: String, dateFormat: String): DataFrame = {
    if (inData == null || timeField == null || date == null) {
      logger.error("Any of the value in getTimeBasedDataFrame is null")
      return null
    }

    val start = TimeUtils.getStartTimestampMS(TimeUtils.getTimeStamp(date, dateFormat))

    val end = TimeUtils.getEndTimestampMS(TimeUtils.getTimeStamp(date, dateFormat))

    val Columns = inData.columns
    if (!(Columns contains (timeField))) {
      logger.error(timeField + "doesn't exist in the inData Frame Schema")
      return null
    }

    val filteredData = inData.filter(timeField + " >= '" + start + "' and " + timeField + " <= '" + end + "'")
    logger.info("Input Data Frame has been filtered before" + start + " after '" + end)
    return filteredData
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

  def getCPOT(dfIn: DataFrame, schema: StructType, dateFormat: String): DataFrame = {

    val dfOpenFiltered = dfIn.select(Udf.allZero2NullUdf(dfIn(dfIn.columns(0)).cast(StringType)) as dfIn.columns(0), Udf.timeToSlot(dfIn(dfIn.columns(1)).cast(StringType), lit(dateFormat)) as dfIn.columns(1))
      .na.drop("any", Array(dfIn.columns(0), dfIn.columns(1)))

    val dfSelect = dfOpenFiltered.sort(dfOpenFiltered.columns(0), dfOpenFiltered.columns(1))

    val mapReduce = dfSelect.map(r => ((r(0), r(1)), 1)).reduceByKey(_ + _)

    val newMap = mapReduce.map{ case (key, value) => (key._1, (key._2.asInstanceOf[Int], value.toInt)) }

    val grouped = newMap.groupByKey().map{ case (key, value) => (key.toString, getCompleteSlotData(value)) }

    val rowRDD = grouped.map({
      case (key, value) =>
        Row(
          key,
          value._1,
          value._2,
          value._3,
          value._4,
          value._5,
          value._6,
          value._7,
          value._8,
          value._9,
          value._10,
          value._11,
          value._12,
          value._13)
    })

    // Apply the schema to the RDD.
    val df = Spark.getSqlContext().createDataFrame(rowRDD, schema)

    df.dropDuplicates()
  }

  /**
   * this method will create a slot data
   * @param iterable
   * @return Tuple2[String, Int]
   */
  def getCompleteSlotData(iterable: Iterable[(Int, Int)]): Tuple13[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int] = {

    logger.info("Enter in getCompleteSlotData:")

    val timeSlotArray = new Array[Int](12)

    var maxSlot: Int = 0

    var max: Int = 0

    iterable.foreach {
      case (slot, value) =>
        if (value > max) { maxSlot = slot; max = value }
        timeSlotArray(slot) = value
    }

    logger.info("Exit from  getCompleteSlotData: ")

    new Tuple13(
      timeSlotArray(0),
      timeSlotArray(1),
      timeSlotArray(2),
      timeSlotArray(3),
      timeSlotArray(4),
      timeSlotArray(5),
      timeSlotArray(6),
      timeSlotArray(7),
      timeSlotArray(8),
      timeSlotArray(9),
      timeSlotArray(10),
      timeSlotArray(11),
      maxSlot)

  }

  /**
   * To generate top map based on dimension.e.g Top brick,brand in the city
   * @param inputDataFrame
   * @param pivotFields
   * @param attributeField
   * @param valueFields
   * @param outputSchema
   * @return
   */
  def generateTopMap(inputDataFrame: DataFrame, pivotFields: Array[String], attributeField: Array[String], valueFields: Array[String], outputSchema: StructType): DataFrame = {
    require(inputDataFrame != null, "input dataframe cannot be null")
    require(Array("count", "sum_price") contains valueFields(0), "value field not supported")

    val keyRdd = inputDataFrame.rdd.keyBy(row => Utils.createKey(row, pivotFields))
    val outRDD = keyRdd.groupByKey().map({ case (key, value) => (key, genMap(value, attributeField, valueFields)) })
      .map{ case (key, value) => (Row.fromSeq(key.toSeq ++ value.toSeq.sortBy(_._1).map(_._2))) }

    val outDataFrame = sqlContext.createDataFrame(outRDD, outputSchema)

    outDataFrame

  }

  /**
   *
   * @param iterable
   * @param dimensions
   * @param values
   * @return
   */
  def genMap(iterable: Iterable[Row], dimensions: Array[String], values: Array[String]): scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, Row]] = {
    val dimensionSMap = scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, Row]]()
    for (row <- iterable) {
      for (dimension <- dimensions) {

        val rowKey = row(row.fieldIndex(dimension)).toString
        val dimensionMap = dimensionSMap.getOrElse(dimension, scala.collection.mutable.Map[String, Row]())
        var rowValue: Row = null
        if (dimensionMap != null) rowValue = dimensionMap.getOrElse(rowKey, null)
        if (dimensionMap != null && rowValue != null) {
          var valueSeq = rowValue.toSeq
          for (value <- values) {
            //          val valueSeq: Seq[Any] = Seq()
            value match {
              case "count" => valueSeq = valueSeq.updated(0, valueSeq.apply(0).asInstanceOf[Int] + 1)
              case "sum_price" => valueSeq = valueSeq.updated(1, valueSeq.apply(1).asInstanceOf[Double] + row(row.fieldIndex(ProductVariables.SPECIAL_PRICE)).asInstanceOf[BigDecimal].doubleValue())
              //            case "avg_price" => valueSeq :+ row(row.fieldIndex(ProductVariables.SPECIAL_PRICE))
            }
          }
          dimensionMap.put(rowKey, Row.fromSeq(valueSeq))

        } else {
          var valueSeq: Seq[Any] = Seq()
          for (value <- values) {

            value match {
              case "count" => valueSeq = valueSeq :+ (1)
              case "sum_price" => valueSeq = valueSeq :+ row(row.fieldIndex(ProductVariables.SPECIAL_PRICE)).asInstanceOf[BigDecimal].doubleValue()
              //            case "avg_price" => valueSeq :+ row(row.fieldIndex(ProductVariables.SPECIAL_PRICE))
            }

          }
          dimensionMap.put(rowKey, Row.fromSeq(valueSeq))

        }
        dimensionSMap.put(dimension, dimensionMap)
      }


    }

    return dimensionSMap

  }

  /**
   * merge two maps
   * @param prevMap
   * @param newMap
   * @return
   */
  def mergeMaps(prevMap: scala.collection.mutable.Map[String, Row], newMap: scala.collection.mutable.Map[String, Row]): scala.collection.mutable.Map[String, Row] = {
    require(prevMap != null && newMap != null, "prevMap and newMap cannot be null")
    if (prevMap == null) return newMap
    if (newMap == null) return prevMap

    newMap.keys.foreach {
      key =>
        if (prevMap.contains(key)) {
          var updatedRow: Row = null
          val prevRowValue = prevMap(key)
          val newRowValue = newMap(key)
          if (newRowValue.size == 1) {
            updatedRow = Row(prevRowValue(prevRowValue.fieldIndex("count")).asInstanceOf[Int] + newRowValue(newRowValue.fieldIndex("count")).asInstanceOf[Int])
          } else {
            updatedRow = Row(prevRowValue(prevRowValue.fieldIndex("count")).asInstanceOf[Int] + newRowValue(newRowValue.fieldIndex("count")).asInstanceOf[Int],
              prevRowValue(prevRowValue.fieldIndex("sum_price")).asInstanceOf[Double] + newRowValue(newRowValue.fieldIndex("sum_price")).asInstanceOf[Double])
          }
          prevMap.put(key, updatedRow)
        } else {
          prevMap.put(key, newMap(key))
        }
    }
    return prevMap
  }

  /**
   *
   * @param a1
   * @param a2
   * @return
   */
  def getNonNull(a1: Any ,a2:Any): Any ={
    if(a1 == null) return a2
    else a1
  }

}
